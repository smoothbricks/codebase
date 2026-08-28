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

From a checkout of this repository, run `cargo build --release -p cowshed-cli` and `nx build cowshed`, then `bun link`
the package. The linked `cowshed` trampoline uses `target/release/cowshed`, while `nx build cowshed` prepares the
TypeScript library and host Node-API addon.

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

| What                                                                | Where                                                                                  |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Images (main + workspaces + checkpoints)                            | `/private/cowshed/store/<owner>/<repo>/` (the primary, component-safe `repo_id`)       |
| Workspace mounts                                                    | `<mount-root>/<owner>/<repo>/<workspace>`                                              |
| Adopted main mount                                                  | its original `<project-root>`                                                          |
| Trusted project policy                                              | `/private/cowshed/store/<owner>/<repo>/policy.json` (controller-owned, sandbox-denied) |
| Repository binding                                                  | `/private/cowshed/store/<owner>/<repo>/repository.json`                                |
| Shared writable build caches (Cargo, sccache, zig, Gradle, Go, Nix) | exact tool subdirectories under `/private/cowshed/caches`                              |
| Gateway registry and repository mirrors                             | `/private/cowshed/caches/{mirror,repo-mirrors}` (gateway-owned, sandbox-read-only)     |
| Host cargo registry (index + `.crate` archives)                     | `~/.cargo/registry/{index,cache}` (host-owned, sandbox-read-only)                      |

Before adoption, cowshed derives a stable lowercase `owner/repo` identity from configured remotes when the choice is
unambiguous. The binding is recorded and revalidated whenever the project opens. Moving the checkout does not change the
identity. Use `--repo-id owner/repo` when a local-only repository or ambiguous remote set cannot supply one.

The `owner` and `repo` components are validated and encoded independently; cowshed never treats a remote string as a
filesystem path. `policy.json` is trusted host policy, not repository content, and is never read from a workspace.

There is no mutable state database. Images/datasets, repository bindings, the kernel mount table, and in-image markers
_are_ the state; every command derives the world by looking at them.

## The cache model in one paragraph

Downloads happen once, ever: the gateway mirrors npm, cargo, and Go module registries (and, via `cowshed repo`, git
repositories) and caches artifacts in `/private/cowshed/caches`. On macOS each workspace's clients use its own localhost
`portBlock.base`; on Linux, where no port block exists, ordinary Bun/Cargo/Go and proxy-aware clients use
`http://127.0.0.1:7644` inside their private netns. A trusted per-workspace connector forwards those bytes only to the
mounted per-workspace Unix gateway socket, which remains the primary endpoint identity. Bun's install cache lives
_inside_ each workspace image — inherited from main via copy-on-write — because bun clones out of it, and clonefile
can't cross volumes; that keeps `bun install` on its fast path. Read-at-build caches are shared under
`/private/cowshed/caches`: Cargo uses distinct writable `cargo/{registry,git}` directories; Go uses `go/{mod,build}`;
Nix uses `nix/{cache,state}`; sccache, zig, and Gradle have named roots. A sandbox's `$CARGO_HOME` follows its private
`HOME`, so it reaches the host's own `~/.cargo/registry/{index,cache}` read-only through links planted at exec, and
unpacks into a writable `registry/src` inside the mount: a crate the host already downloaded builds offline in every
workspace. Gateway artifacts are not Cargo caches: registry objects live under `mirror/` and bare repository mirrors
under `repo-mirrors/`, both gateway-owned and read-only to workspaces. On declarative hosts, the system/home-manager
module owns all relocations, including `~/.cache/nix → nix/cache` and `~/.local/state/nix → nix/state`; cowshed only
validates them. `cowshed adopt --imperative-host-setup` is an explicit exception for non-declarative hosts, never an
automatic fallback after declarative validation fails.

## Reusing compiled output across workspaces

Two different mechanisms save build time, and it helps to keep them apart.

**The clone is why a workspace starts warm.** `cowshed new` copies main's image, `target/` included, so the compiler is
never asked about code that did not change — the build tool simply finds its own previous output already there. Nothing
is looked up in a cache for this; it is the copy-on-write clone doing the work.

**The compile cache is for the work that is left.** When a workspace does have to compile something — its own edits, or
whatever landed on main since the clone — the host compile-cache daemon can hand back an object another workspace or
main already produced. That only works if the cache key ignores where the workspace happens to be mounted, because every
workspace sits at a different path.

### What cowshed contributes

| Choice                                                    | Why it matters for reuse                                                                                                                                                       |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `target/` lives inside each workspace image               | Each agent gets a private warm build directory. A shared external build directory would serialise every build on one lock instead.                                             |
| One host daemon owns the compile cache                    | `cowshed sccache start` pins the store path and the size cap. A build that starts its own server instead gets a small default cap and evicts what the next workspace came for. |
| Cache keys are normalised relative to the build directory | This is what lets a workspace at one mount path use an object produced at another. `cowshed sccache status` reports whether it is working.                                     |
| `--slot <n>` recycles a stable mount path                 | For any cache that is keyed by path rather than by content.                                                                                                                    |
| Registry and module downloads are shared, read-only       | Dependencies are fetched once per host, never once per workspace.                                                                                                              |

### The rules that make it work for Rust

| Rule                                                                              | Why                                                                                                                                                                                         |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Never set `CARGO_INCREMENTAL`                                                     | Set to `1` it aborts the build outright; set to `0` it throws away incremental compilation and gains nothing. Left unset, your own crates stay incremental while everything else is shared. |
| Shared lanes are not incremental: `[profile.test] incremental = false`            | Incremental output is the one kind that can never be shared. Without this line, no test build is reusable.                                                                                  |
| Shared lanes carry no debuginfo: `debug = 0`                                      | A reused object keeps the paths of the checkout that produced it, so a shared debug build shows another workspace's source paths.                                                           |
| The `dev` profile stays incremental and local                                     | That is where debuginfo belongs, and your own in-progress edits could never match someone else's object anyway.                                                                             |
| No compile-time absolute paths in shipped code, e.g. `env!("CARGO_MANIFEST_DIR")` | The path is baked into the output, so the crate and everything above it stops matching at any other mount path.                                                                             |
| Nothing machine-specific above the build directory in `.cargo/config.toml`        | An absolute `linker`, `rustflags` entry, `[env]` value, or `target-dir` outside the checkout pins the key to one machine.                                                                   |
| One toolchain, owned by devenv                                                    | A different compiler is a different cache. `rust-toolchain.toml` does not do this here — nix's cargo ignores it, so it silently disagrees with the shell.                                   |
| Do not point cargo at a shared external `target/`                                 | It undoes the isolation the clone gave you and serialises builds.                                                                                                                           |
| `-C target-cpu=native` ties the cache to one CPU class                            | Fine for a single host; it means the store cannot be shared with a different machine generation.                                                                                            |

Two things that look like fixes but are not: `trim-paths` and `--remap-path-prefix` do make artifacts path-neutral, but
they change the cache key too, so the reuse you were buying disappears. Use `debug = 0` on shared lanes instead.

`smoo monorepo check` enforces the manifest and config rules above, and the managed devenv module supplies the
environment they assume.

### The same rules for other toolchains

Ask three questions of any build cache before sharing it between workspaces:

1. Is the key derived from content rather than from where the files live?
2. Is a reused artifact still correct at a different path?
3. Does one process own the store and its size cap?

| Toolchain           | How it lands                                                                                                                                                |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Go                  | `GOCACHE` and `GOMODCACHE` are content-addressed, so they share safely. Build with `-trimpath` for path-neutral binaries.                                   |
| TypeScript via ttsc | `TTSC_CACHE_DIR` holds content-keyed plugin binaries; keep it outside `node_modules` so installs stay lean.                                                 |
| Bun                 | The install cache stays inside each workspace image on purpose: `bun install` clones out of it, and clones cannot cross volumes.                            |
| Nix                 | Content-addressed by definition; the store is shared and read-only to workspaces.                                                                           |
| Zig, Gradle         | Named roots under `/private/cowshed/caches`; the same three questions apply.                                                                                |
| C and C++ via cc-rs | Absolute include or SDK paths reach the compiler the same way a Rust linker path does — keep them below the build directory or resolve them through `PATH`. |

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
