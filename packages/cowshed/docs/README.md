# cowshed — warm git workspaces

cowshed gives you **instant, isolated, warm workspaces** for any git repository. A cowshed workspace is a full
standalone checkout — source, `.git`, `node_modules`, `target/`, every build cache — cloned copy-on-write from your live
main workspace in milliseconds. Work in it, run agents in it, destroy it. The host filesystem gains one lightweight
object per workspace instead of a hundred thousand inodes.

## Platforms

The working first product path is macOS with APFS images and Seatbelt. Linux with ZFS, Landlock, and private network
namespaces is a later platform goal; [zfs.md](zfs.md) records that contract but is not part of the basic macOS setup.

| Platform | Substrate                       | Status                                        |
| -------- | ------------------------------- | --------------------------------------------- |
| macOS    | APFS image per workspace        | Current implementation and integration target |
| Linux    | ZFS dataset clone per workspace | Subsequent Linux goal                         |

```sh
$ cd ~/src/api
$ cowshed new raven --json
{"ok":true,"result":{"workspace":"raven","mount":"<mount-root>/acme/api/raven","baseCommit":"6f3a2c1000000000000000000000000000000000"}}
next: cowshed exec raven -- <cmd>
```

The JSON line is the only stdout. `next:` is stderr guidance. That split holds for every command: **stdout is for
machines; stderr is for humans and agents deciding what to do next.**

## Why

- **Copy-on-write.** `cowshed new` clones an image instead of recursively copying or registering a linked worktree.
  `cowshed rm` retires one storage object instead of walking tens of thousands of files.
- **Warm.** Each adopted repository has its own live `main` image. Workspaces inherit that repository's source,
  standalone `.git`, materialized dependencies, and build state.
- **Isolated.** Every workspace is an independent volume with a standalone Git checkout. `cowshed exec` applies the
  workspace sandbox, sanitized environment, controller-selected caches, and gateway endpoint.
- **Repository-aware.** A host may adopt many repositories. cwd or `--project <git-root>` selects which repository's
  `main` to clone; `--from <workspace>` selects another source inside that repository.
- **Inode-friendly.** Dependency and build trees live inside image files rather than expanding the host Data volume's
  inode namespace.

## Install

The `cowshed` command is the `bin` of the `@smoothbricks/cowshed` npm package. Its exec trampoline launches the prebuilt
Rust executable for the host platform without starting Node-API; a checkout's `target/release/cowshed` is the
local-development fallback, and the Node-API addon's `runCli` remains the final compatibility fallback. The npm package
contains the same four macOS/Linux architecture artifacts as the library's native-addon matrix, so the CLI and library
are versioned and published together.

```sh
bunx @smoothbricks/cowshed doctor      # one-off
bun add --global @smoothbricks/cowshed # `cowshed` on PATH
```

From a checkout of this repository, run `nx build cowshed -c production`, then `bun link` the package. The production
configuration builds the host's release CLI into `dist/bin/<platform>/cowshed`, which the linked `cowshed` trampoline
runs; the default configuration builds cargo's dev profile, which is for tests and local iteration, not for the binary
on `PATH`. The same build prepares the TypeScript library and host Node-API addon.

### The agent skill

`cowshed skill install` writes the bundled skill into every agent harness detected on the host, and
`cowshed skill install --project <path>` installs it into one repository. It is idempotent, needs no network, and works
before `adopt` has run.

Its harness table is a generated snapshot of [vercel-labs/skills](https://github.com/vercel-labs/skills), refreshed with
`nx run cowshed:refresh-harnesses`. The generated file records the upstream revision it came from and lists the entries
whose paths could not be reduced to a literal home path. Hand-verified entries in `VERIFIED_HARNESSES` override that
snapshot by name and carry the evidence for doing so.

For a harness outside the snapshot, install the shipped skill directory with the upstream tool instead:

```sh
npx skills add ./skills/cowshed -g
```

## Five-minute quickstart

```sh
# 1. One-time: convert this checkout into its repository-scoped warm main.
cd <project-root>
cowshed adopt

# Local-only repositories use an explicit identity:
# cowshed adopt <project-root> --repo-id owner/repo

# 2. Start the managed gateway.
cowshed gateway start

# 3. Create a warm workspace from this repository's main.
WS=raven
MOUNT=$(cowshed new "$WS")

# 4. Work normally, or run autonomous commands under the sandbox.
cd "$MOUNT"
cowshed exec "$WS" -- bun test

# 5. Land the branch into main; that retires the workspace.
cowshed land "$WS"
```

From outside the repository, make selection explicit:

```sh
cowshed new raven --project <project-root>
```

For the complete repository-selection model, multi-repository examples, agent loop, JSON contract, and safe cleanup
rules, start with [usage.md](usage.md).

## Where things live

| What                                                                                                      | Where                                                                                  |
| --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Images (main + workspaces + checkpoints)                                                                  | `/private/cowshed/store/<owner>/<repo>/` (the primary, component-safe `repo_id`)       |
| Workspace mounts                                                                                          | `<mount-root>/<owner>/<repo>/<workspace>`                                              |
| Adopted main mount                                                                                        | its original `<project-root>`                                                          |
| Trusted project policy                                                                                    | `/private/cowshed/store/<owner>/<repo>/policy.json` (controller-owned, sandbox-denied) |
| Repository binding                                                                                        | `/private/cowshed/store/<owner>/<repo>/repository.json`                                |
| Shared writable build caches of detected tools (Cargo, Bun, npm, pnpm, uv, sccache, zig, Gradle, Go, Nix) | exact tool subdirectories under `/private/cowshed/caches`                              |
| Gateway registry and repository mirrors                                                                   | `/private/cowshed/caches/{mirror,repo-mirrors}` (gateway-owned, sandbox-read-only)     |
| Host cache directories (Cargo, Bun, npm, pnpm, uv, zig, Gradle, Nix)                                      | symlinks into `/private/cowshed/caches` (`cowshed setup --imperative-host-setup`)      |

Before adoption, cowshed derives a stable lowercase `owner/repo` identity from configured remotes when the choice is
unambiguous. The binding is recorded and revalidated whenever the project opens. Moving the checkout does not change the
identity. Use `--repo-id owner/repo` when a local-only repository or ambiguous remote set cannot supply one.

The `owner` and `repo` components are validated and encoded independently; cowshed never treats a remote string as a
filesystem path. `policy.json` is trusted host policy, not repository content, and is never read from a workspace.

There is no mutable state database. Images/datasets, repository bindings, the kernel mount table, and in-image markers
remain authoritative. A bounded `lifecycle-intents.json` journal records the latest create/fork/remove intent for each
logical workspace before mutation; startup reconciles it against that authoritative inventory so a killed caller can
retry without duplicating or wedging the operation.

## The cache model in one paragraph

Downloads happen once, ever: registry artifacts the gateway mirrors are cached in `/private/cowshed/caches`, and the
shared tool caches below keep what cargo and Go download. On macOS each workspace's clients use its own localhost
`portBlock.base` as their proxy; on Linux, where no port block exists, proxy-aware clients — Bun, Cargo, Go and git —
use `http://127.0.0.1:7644` inside their private netns. A trusted per-workspace connector forwards those bytes only to
the mounted per-workspace Unix gateway socket, which remains the primary endpoint identity. Shared caches live under
`/private/cowshed/caches`, and a sandbox writes only those of the tools its project uses (15_capabilities.md): Cargo
uses distinct writable `cargo/{registry,git}` directories; Bun's global install cache is `bun/install/cache`; npm's is
`npm` and pnpm's `pnpm/store`; uv's is `uv`; Go uses `go/{mod,build}`; Nix uses `nix/{cache,state}`; sccache, zig, and
Gradle have named roots. `cowshed setup --imperative-host-setup` moves the host's own caches there and links them back,
and from then on every sandbox uses the host's own paths — `CARGO_HOME=~/.cargo`,
`BUN_INSTALL_CACHE_DIR=~/.bun/install/cache`, `UV_CACHE_DIR=~/.cache/uv`. One literal path is what makes sharing work:
cargo fingerprints a dependency by the absolute path of its source under `$CARGO_HOME`, so a clone's copied `target/`
stays fresh only against the same path, and Bun's isolated linker writes its cache path into every `node_modules/.bun`
link, so main's `node_modules` and every clone's resolve only against the same cache. A crate or package anything on the
host has already downloaded installs offline in every workspace. Gateway artifacts are not tool caches: registry objects
live under `mirror/` and bare repository mirrors under `repo-mirrors/`, both gateway-owned and read-only to workspaces.
On declarative hosts, the system/home-manager module owns all relocations, including `~/.cache/nix → nix/cache` and
`~/.local/state/nix → nix/state`; cowshed only validates them. `cowshed setup --imperative-host-setup` is an explicit
exception for non-declarative hosts, never an automatic fallback after declarative validation fails.

## Reusing compiled output across workspaces

Two different mechanisms save build time, and it helps to keep them apart.

**The clone is why a workspace starts warm.** `cowshed new` clones the target's source image and its latest immutable
build-volume seed. Cargo target directories, Nx state and per-tree indexers live in that separate private APFS image,
reached through `.cowshed/build` and fixed relative links. No file copy or warm-build step is part of fork or land.

**Existing checkouts migrate by rebuilding, never copying.** Setup and the first admitted job create an empty build
volume, publish `.cowshed/build`, discard only contributed real build-state directories, and install their fixed links.
The state file and image sidecar are written last, so an interrupted migration resumes on the same volume. One Git query
checks every directory before any deletion: tracked files under a configured target directory refuse migration and name
the protected paths. The next build repopulates the volume; a land freezes the warm seed for later forks. If a tool such
as `cargo clean` replaces a fixed link with a real directory, the next admission reports it, applies the same
tracked-source guard, discards that rebuildable directory and restores the link. Files and foreign links refuse.

**The compile cache is for the work that is left.** When a workspace does have to compile something — its own edits, or
whatever landed on main since the clone — the host compile-cache daemon can hand back an object another workspace or
main already produced. That only works if the cache key ignores where the workspace happens to be mounted, because every
workspace sits at a different path.

### What cowshed contributes

| Choice                                                    | Why it matters for reuse                                                                                                                                                          |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Build state lives in a private build-volume clone         | Each checkout has one mutable volume and one immutable latest seed; a land moves one link instead of copying caches. Shared build directories would serialise builds on one lock. |
| One host daemon owns the compile cache                    | `cowshed sccache start` pins the store path and the size cap. A build that starts its own server instead gets a small default cap and evicts what the next workspace came for.    |
| Cache keys are normalised relative to the build directory | This is what lets a workspace at one mount path use an object produced at another. `cowshed sccache status` reports whether it is working.                                        |
| `--slot <n>` recycles a stable mount path                 | For any cache that is keyed by path rather than by content.                                                                                                                       |
| Registry and module downloads are shared and writable     | Dependencies are fetched once per host; each detected tool may maintain its own cache safely from the sandbox. Gateway mirrors remain read-only.                                  |

### The rules that make it work for Rust

| Rule                                                                        | Why                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Never set `CARGO_INCREMENTAL`                                               | Set to `1` it aborts the build outright; set to `0` it throws away incremental compilation and gains nothing. Left unset, your own crates stay incremental while everything else is shared.                                                                                                                                                                                            |
| `test` inherits `dev`: no `[profile.test]` overrides                        | `cargo test` and `cargo build` then compile each dependency once, as the same unit, so the `target/` a workspace clones from main is warm for both. A test profile that differs from `dev` builds every dependency twice.                                                                                                                                                              |
| `dev` stays incremental                                                     | An edit rebuilds only what it touched in your own crates. Cargo changes workspace unit identities when `CI` is set. Cowshed's source-able `.cowshed/env`, one-shot jobs and warm exec overlays strip agent-harness `CI`; real GitHub/Forgejo Actions (`GITHUB_ACTIONS=true`) keeps it. Compiler/toolchain variables are preserved, so host shells and sandbox jobs use the same units. |
| No compile-time checkout path, tests included: `env!("CARGO_MANIFEST_DIR")` | Cargo does not fingerprint where the checkout lives, so a workspace runs the units it cloned from main as they are: a baked path makes a test read main's files, not the workspace's. Read `CARGO_MANIFEST_DIR` at run time (cargo and nextest set it for every test) and name compiled-in files relative to their source.                                                             |
| Nothing machine-specific above the build directory in `.cargo/config.toml`  | An absolute `linker`, `rustflags` entry, `[env]` value, or `target-dir` outside the checkout pins the key to one machine.                                                                                                                                                                                                                                                              |
| One toolchain for every checkout                                            | A different compiler is a different cache. Pin it once in the project's own environment, and make sure every shell and sandbox resolves the same `rustc`: a `rust-toolchain.toml` that one toolchain honours and another ignores silently splits the cache.                                                                                                                            |
| Do not point cargo at a shared external `target/`                           | It undoes the isolation the clone gave you and serialises builds.                                                                                                                                                                                                                                                                                                                      |
| `-C target-cpu=native` ties the cache to one CPU class                      | Fine for a single host; it means the store cannot be shared with a different machine generation.                                                                                                                                                                                                                                                                                       |

Two things you do not need to set: `trim-paths` and `--remap-path-prefix`. The bundled sccache already compiles every
unit it shares across checkouts with the checkout root remapped away, and stores a unit whose output still names its
checkout (a proc macro that reads `CARGO_MANIFEST_DIR`, say) for that checkout alone. Setting either yourself puts the
checkout path into the key, so the reuse disappears.

### The same rules for other toolchains

Ask three questions of any build cache before sharing it between workspaces:

1. Is the key derived from content rather than from where the files live?
2. Is a reused artifact still correct at a different path?
3. Does one process own the store and its size cap?

| Toolchain           | How it lands                                                                                                                                                                                                                                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Go                  | `GOCACHE` and `GOMODCACHE` are content-addressed, so they share safely. Build with `-trimpath` for path-neutral binaries. In a Go project (`go.mod` or `go.work`) every sandboxed job names both under `/private/cowshed/caches/go/{build,mod}` whenever that root exists.                                               |
| TypeScript via ttsc | `TTSC_CACHE_DIR` holds content-keyed plugin binaries, outside `node_modules` so installs stay lean. Cowshed has no ttsc capability, so a sandbox writes no shared ttsc root: keep the cache inside the checkout (`.cache/ttsc`), where every clone inherits main's warm copy through the image.                          |
| Bun                 | Shared: the isolated linker writes `node_modules/.bun` as links into the install cache, so every checkout reaches it through `~/.bun/install/cache`.                                                                                                                                                                     |
| Python via uv       | The cache is shared through `~/.cache/uv`, like Bun's. A clone enters with the environment it copied only if that environment is free of its path — relocatable scripts, editable installs relative to site-packages, an interpreter whose path names no checkout.                                                       |
| Nix                 | Content-addressed by definition; the store is shared and read-only to workspaces.                                                                                                                                                                                                                                        |
| Zig, Gradle         | Named roots under `/private/cowshed/caches`; the same three questions apply.                                                                                                                                                                                                                                             |
| Nx                  | An Nx project (`nx.json`) has one `.nx/cache`, one `.nx/workspace-data` and one daemon/socket namespace across host, read-write and source-read-only jobs. Both state directories are fixed links into its private build volume. Caller Nx directory overrides never pass; inherited daemon records are deleted at fork. |
| C and C++ via cc-rs | `HOST_CC`/`HOST_CXX` are `sccache cc` (never `CC`/`CXX` — xcodebuild reads those). Absolute include or SDK paths still have to sit below the build directory.                                                                                                                                                            |

## Documentation

- [usage.md](usage.md) — start here: repository selection, multi-main mental model, daily and agent workflows
- [cli.md](cli.md) — command guide, stdout/stderr contract, exit codes, grants
- [agents.md](agents.md) — driving cowshed from coding agents
- [gateway.md](gateway.md) — gateway setup, credentials, mirrors, egress allowlists
- [ios.md](ios.md) — iOS/Expo development across the dev-uid boundary: simulators, the drop dir, the `xcrun` wrapper
- [desktop.md](desktop.md) — macOS desktop apps across the dev-uid boundary: the three lanes (test/debug as dev, use as
  you) and `app promote`
- [zfs.md](zfs.md) — Linux/ZFS substrate: pool setup, send/receive, pinned-space lifecycle
- [ci.md](ci.md) — cowshed as a self-hosted GitHub Actions runner
- [troubleshooting.md](troubleshooting.md) — mounts, sandbox denials, disk usage, backup story

Design rationale and tradeoffs live in `specs/cowshed/` at the repository root.
