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

The workspace and test targets must still compile on Linux. Pure retained-input path witnesses are available across
crate boundaries on every platform; disk-image attachment, native port allocation, and Darwin process-argument
diagnostics remain macOS-only. From the repository root, `bun run check:linux` checks the full Linux target surface.
Shell fixtures running under cleared child environments supply their tool PATH from the test environment rather than
assuming coreutils live under `/bin` or `/usr/bin`.

```sh
$ cd ~/src/api
$ cowshed fork raven --json
{"ok":true,"result":{"workspace":"raven","mount":"<mount-root>/acme/api/raven","baseCommit":"6f3a2c1000000000000000000000000000000000"}}
next: cowshed exec raven -- <cmd>
```

The JSON line is the only stdout. `next:` is stderr guidance. That split holds for every command: **stdout is for
machines; stderr is for humans and agents deciding what to do next.**

## Why

- **Copy-on-write.** `cowshed fork` clones an image instead of recursively copying or registering a linked worktree.
  `cowshed rm` retires one storage object instead of walking tens of thousands of files.
- **Warm.** Each adopted repository has its own live `main` image. Workspaces inherit that repository's source,
  standalone `.git`, materialized dependencies, and build state.
- **Isolated.** Every workspace is an independent volume with a standalone Git checkout. `cowshed exec` applies the
  workspace sandbox, sanitized environment, controller-selected caches, and gateway endpoint.
- **Repository-aware.** A host may adopt many repositories. cwd or `--project <git-root>` selects which repository's
  `main` to clone by default; `fork <src> <ws>` selects another source inside that repository.
- **Inode-friendly.** Dependency and build trees live inside image files rather than expanding the host Data volume's
  inode namespace.

## Install

The `cowshed` command is the `bin` of the `@smoothbricks/cowshed` npm package. Its launcher, a shell script, execs the
prebuilt Rust executable for the host platform without starting Node: a published package's
`dist/bin/<platform>/cowshed`, or a checkout's development build, `.cache/bin/<platform>/cowshed`
([06 CLI](../../../specs/cowshed/06_cli.md#launcher)). The npm package contains the same four macOS/Linux architecture
artifacts as the library's native-addon matrix, so the CLI and library are versioned and published together.

```sh
bunx @smoothbricks/cowshed doctor      # one-off
bun add --global @smoothbricks/cowshed # put the package's `cowshed` on PATH
cowshed setup                          # install that build; `cowshed` on PATH now runs the installed copy
```

`cowshed setup` installs the build it runs from as the host's cowshed: a content-addressed copy under
`~/Library/Application Support/dev.cowshed/bin`, which the gateway daemon runs and which the first `cowshed` on your
`PATH` is pointed at when that entry is a symbolic link. The daemon and the `cowshed` you type are then the same bytes,
and only another install changes them ([gateway.md](gateway.md#start-at-login-launchd)). A later `cowshed setup` typed
at the shell runs that installed copy, so it repairs the host but installs nothing new.

The managed environment is published from the controller's trusted host installation, not from the private `HOME` inside
a sandbox. Sandbox profiles permit only the current installed binary to be read and executed, through its stable name
with lookup metadata; the install directory and controller records remain closed to reads/writes, and the gateway
control socket remains host-only ([04 Sandbox](../../../specs/cowshed/04_sandbox.md), installed-cowshed exception).

Nothing installed answers to `cargo`, and setup removes the `cargo -> cowshed` link earlier installs planted. A job's
jobserver reaches Cargo as configuration Cargo already reads: the job's supervisor exports `CARGO_MAKEFLAGS` naming the
job's own make jobserver fifo, and the test-thread counts. A Cargo no job started runs as it does unconfigured
([03 Caches](../../../specs/cowshed/03_caches.md), "Cargo reads its budget from its holder";
[05 Gateway](../../../specs/cowshed/05_gateway.md#parallelism-inside-a-job)).

From a checkout of this repository, build the host's release CLI with `nx run cowshed:cli-<platform>:production`
(`cli-arm64-macos` on Apple silicon), which writes `dist/bin/<platform>/cowshed`, then install it by running that
build's own setup, `packages/cowshed/dist/bin/<platform>/cowshed setup`. A checkout's `bin/cowshed` runs its development
build, `.cache/bin/<platform>/cowshed` (`nx run cowshed:cli-native`), which setup refuses. After that, `cowshed` on
`PATH` is the installed copy, not the checkout: a later rebuild changes nothing there until you run the release build's
`setup` again. Do not put a link to the checkout's `bin/cowshed` on `PATH` — it would run whatever the checkout last
built, under a daemon still running the build setup installed, and the daemon refuses another build's commands. The
default `cli-*` configuration builds cargo's dev profile, which is for tests and local iteration, and setup refuses to
install it; setup also refuses a build that does not contain the installed one — its commit must be on `main` and hold
every installed commit — unless you pass `--downgrade`.

A daemon/caller “the cowshed daemon is build X; this cowshed is build Y” refusal (exit 4) is a build-identity conflict,
not workspace corruption; install the intended build with its own setup before retrying
([05 Gateway](../../../specs/cowshed/05_gateway.md), “One artifact”).

### The agent skill

`cowshed skill install` writes the bundled skill into every agent harness detected on the host, and
`cowshed skill install --project <path>` installs it into one repository. It is idempotent, needs no network, and works
before `adopt` has run. `cowshed setup` writes the same skill into every harness present whenever it installs a cowshed,
so the guidance agents read is always the installed cowshed's own; a copy that already holds it is left alone.

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
MOUNT=$(cowshed fork "$WS")

# 4. Work normally, or run autonomous commands under the sandbox.
cd "$MOUNT"
cowshed exec "$WS" -- bun test

# 5. Land the branch into main; that retires the workspace.
cowshed land "$WS"
```

From outside the repository, make selection explicit:

```sh
cowshed fork raven --project <project-root>
```

For the complete repository-selection model, multi-repository examples, agent loop, JSON contract, and safe cleanup
rules, start with [usage.md](usage.md).

`cowshed mv <workspace> <new-name>` preserves the branch, staged index, uncommitted files and ignored warm state. An
unfinished rename owns both names until it completes. Removing its source discards a prepared rename only when nothing
was published; once it has mutated, removal refuses and names `cowshed rm <new-name> --force` as the cancellation that
keeps the source.

## Where things live

| What                                                                                             | Where                                                                                                 |
| ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| Images (main + workspaces)                                                                       | `/private/cowshed/store/<owner>/<repo>/` (the primary, component-safe `repo_id`)                      |
| Workspace mounts                                                                                 | `<mount-root>/<owner>/<repo>/<workspace>`                                                             |
| Adopted main mount                                                                               | its original `<project-root>`                                                                         |
| Trusted project policy                                                                           | `/private/cowshed/store/<owner>/<repo>/policy.json` (controller-owned, sandbox-denied)                |
| Repository binding                                                                               | `/private/cowshed/store/<owner>/<repo>/repository.json`                                               |
| Shared writable build caches of detected tools (Cargo, Bun, npm, pnpm, uv, zig, Gradle, Go, Nix) | each tool's own default in your HOME (`~/.cargo/registry`, `~/.bun/install/cache`, `~/go/pkg/mod`, …) |
| Repository-placed caches declared in main's `.cowshed.toml` `[caches] home`                      | the declared HOME-relative path (`~/<path>`), linked from each sandbox's private HOME                 |
| mbx compile cache                                                                                | `mbx` on the `cowshed.build-cache` volume (shared, writable by mbx projects' jobs)                    |
| Gateway registry and repository mirrors                                                          | `~/Library/Caches/dev.cowshed/{mirror,repo-mirrors}` (gateway-owned, sandbox-denied)                  |

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

Downloads happen once, ever: registry artifacts the gateway mirrors are cached in `~/Library/Caches/dev.cowshed/mirror`,
and the shared tool caches below keep what cargo and Go download. On macOS each workspace's clients use its own
localhost `portBlock.base` as their proxy; on Linux, where no port block exists, proxy-aware clients — Bun, Cargo, Go
and git — use `http://127.0.0.1:7644` inside their private netns. A trusted per-workspace connector forwards those bytes
only to the mounted per-workspace Unix gateway socket, which remains the primary endpoint identity. Shared caches are
the tools' own, each at its default in your HOME, and a sandbox writes only those of the tools its project uses
(15_capabilities.md): Cargo's `~/.cargo/{registry,git}`, Bun's `~/.bun/install/cache`, npm's `~/.npm`, pnpm's
`~/Library/pnpm/store`, uv's `~/.cache/uv`, Go's `~/go/pkg/mod` and `~/Library/Caches/go-build`, Zig's `~/.cache/zig`,
Gradle's `~/.gradle/caches`, Nix's `~/.cache/nix` and `~/.local/state/nix`. The rest of HOME stays denied. Every sandbox
uses the host's own paths — `CARGO_HOME=~/.cargo`, `BUN_INSTALL_CACHE_DIR=~/.bun/install/cache`,
`UV_CACHE_DIR=~/.cache/uv` — so the bytes a sandbox writes are the bytes your own `cargo`, `bun` or `go` reads next. One
literal path is what makes sharing work: cargo fingerprints a dependency by the absolute path of its source under
`$CARGO_HOME`, so a clone's copied `target/` stays fresh only against the same path, and Bun's isolated linker writes
its cache path into every `node_modules/.bun` link, so main's `node_modules` and every clone's resolve only against the
same cache. A crate or package anything on the host has already downloaded installs offline in every workspace. Gateway
artifacts are not tool caches: registry objects live under `mirror/` and bare repository mirrors under `repo-mirrors/`,
both written only by the gateway and unreachable from workspaces. Nothing is relocated or linked, so a declarative host
has nothing to declare for these caches. A cache your repository's own tooling places, which no detector names, is
shared by naming its HOME-relative path in main's `.cowshed.toml` — `[caches] home = [".cache/ttsc"]` — trusted only
from main, like `[sandbox] deny`.

The immutable `/nix/store` stays read-only and readable without a grant, even in a project with no Nix capability. On
macOS this includes metadata as well as file contents: dyld must resolve the separate store entries containing a tool's
shared libraries before it can load them. Nix daemon access and writable client caches still come from the Nix
capability, not from store reads
([sandbox read policy](../../../specs/cowshed/04_sandbox.md#devenv--nix-inside-the-sandbox)).

## Reusing compiled output across workspaces

Two different mechanisms save build time, and it helps to keep them apart.

**The clone is why a workspace starts warm.** A fork clones the target's source image and its latest immutable
build-volume seed. Nx state, and Cargo target directories in a project without mbx, live in that separate private APFS
image (an mbx project's `target` is mbx's managed-target link instead), reached through `.cowshed/build` and fixed
relative links; `.codegraph` is one of them, a link to `.cowshed/build/codegraph`, so the index is build-volume state
carried like the rest. Every gated land freezes a seed from the landing workspace's warm volume; an ungated lane fold
(`land --into <lane> --ungated`) moves Git alone and leaves the lane base's seed as it was. A plain Git fast-forward is
detected at the next fork, including `--ref` at the target's current tree: a quiet live workspace at exactly that tree
seeds the target automatically. Between lands, the fork captures newer live state only when it is at least as near the
requested tree and has no writer. A nearer landed seed beats an older live volume. There is no manual seed-refresh or
warm step. `doctor` reports target-only `seed-age`, `seed-tree-behind` and `target-moved-outside-land`; land records its
actual seeded/adopted/skipped outcome durably, including holders. An owned index link points back to source only through
a verified sealed snapshot; a holder refuses that change without losing bytes or being forced off.

**A fresh baseline carries source and build state together.** Fork from the newer baseline instead of replacing an
existing workspace's build volume. Rebase and land keep that workspace's own build state, so ordinary mtime-based tools
observe source edits without a tool-specific freshness mode.

**Existing checkouts migrate by rebuilding, never copying.** Setup and the first admitted job create an empty build
volume, publish `.cowshed/build`, discard only contributed real build-state directories, and install their fixed links.
The state file and image sidecar are written last, so an interrupted migration resumes on the same volume. One Git query
checks every directory before any deletion: tracked files under a configured target directory refuse migration and name
the protected paths. The next build repopulates the volume; a land freezes the warm seed for later forks. If a tool such
as `cargo clean` replaces a fixed link with a real directory, the next admission reports it, applies the same
tracked-source guard, discards that rebuildable directory and restores the link. Files and foreign links refuse.

**State no capability detects is declared.** `.cowshed.toml` `[build] state = ["vendor/upstream"]` puts each listed
checkout path on the build volume too, for tool state no convention file names — a gitignored patch-development checkout
of an upstream project and its multi-GiB incremental build tree is the case it exists for. The path must be untracked,
inside the checkout and apart from every detected tool's state; anything else refuses with the remedy. It is
rebuild-only like all build state, but cowshed cannot see what a declared directory holds, so it never deletes one, and
`rm` and `land` refuse the workspace while one is kept: the first admission after you declare it moves the existing
directory aside intact into `.cowshed/kept/` and links the path onto the volume. Take what you need from the kept
directory, re-run whatever reconstructs the state once, and delete the kept directory; from then on it lives on the
volume and every fork gets it warm. A declared path that does not exist stays absent (tools read its existence as
meaning) and is linked at the first admission after something creates it. A pattern such as `packages/*/.cache/lmao`
declares the name beneath every directory the checkout tracks files under, so a package added later joins on its own.

**The compile cache is for the work that is left.** When a workspace does have to compile something — its own edits, or
whatever landed on main since the clone — mbx's shared store can hand back a unit another workspace or main already
produced. That only works if the unit's key ignores where the workspace happens to be mounted, because every workspace
sits at a different path.

### What cowshed contributes

| Choice                                                | Why it matters for reuse                                                                                                                                                          |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Build state lives in a private build-volume clone     | Each checkout has one mutable volume and one immutable latest seed; a land moves one link instead of copying caches. Shared build directories would serialise builds on one lock. |
| One mbx store per host                                | `cowshed setup` installs the mbx and Cargo shim every Cargo project runs, whatever mbx it pins, so a project is never given a second store.                                       |
| Units keyed by their inputs, not their checkout       | This is what lets a workspace at one mount path use a unit produced at another. No wrapper sits between Cargo and rustc.                                                          |
| `--slot <n>` recycles a stable mount path             | For any cache that is keyed by path rather than by content.                                                                                                                       |
| Registry and module downloads are shared and writable | Dependencies are fetched once per host; each detected tool may maintain its own cache safely from the sandbox. Gateway mirrors remain read-only.                                  |

### The rules that make it work for Rust

| Rule                                                                        | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Never set `CARGO_INCREMENTAL`                                               | Incremental compilation is mbx's; a committed or exported value overrides it for every checkout.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `test` inherits `dev`: no `[profile.test]` overrides                        | `cargo test` and `cargo build` then compile each dependency once, as the same unit, so the `target/` a workspace clones from main is warm for both. A test profile that differs from `dev` builds every dependency twice.                                                                                                                                                                                                                                                                                                                                                           |
| `dev` stays incremental                                                     | An edit rebuilds only what it touched in your own crates. Cargo changes workspace unit identities when `CI` is set. Cowshed's source-able `.cowshed/env`, one-shot jobs and warm exec overlays strip agent-harness `CI`; real GitHub/Forgejo Actions (`GITHUB_ACTIONS=true`) keeps it. Compiler/toolchain variables are preserved, so host shells and sandbox jobs use the same units.                                                                                                                                                                                              |
| Every workspace root declares the `dev` and `debugging` profiles            | `dev` gives your crates file:line debuginfo (`line-tables-only`, `split-debuginfo = "unpacked"`) and dependencies none (`package."*"` `debug = 0`): measured −57% target bytes at flat CPU. `build-override` carries the same debug level as `package."*"`, or a crate used by both build scripts and ordinary code compiles twice. To debug, `cargo build --profile debugging` (or `cargo test --profile debugging`): `dev` with `debug = 2` everywhere, in `target/debugging`, so it never invalidates a dev unit. `smoo monorepo validate` prints the TOML for anything missing. |
| No compile-time checkout path, tests included: `env!("CARGO_MANIFEST_DIR")` | Cargo does not fingerprint where the checkout lives, so a workspace runs the units it cloned from main as they are: a baked path makes a test read main's files, not the workspace's. Read `CARGO_MANIFEST_DIR` at run time (cargo and nextest set it for every test) and name compiled-in files relative to their source.                                                                                                                                                                                                                                                          |
| Nothing machine-specific above the build directory in `.cargo/config.toml`  | An absolute `linker`, `rustflags` entry, `[env]` value, or `target-dir` outside the checkout pins the key to one machine.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| One toolchain for every checkout                                            | A different compiler is a different cache. Pin it once in the project's own environment, and make sure every shell and sandbox resolves the same `rustc`: a `rust-toolchain.toml` that one toolchain honours and another ignores silently splits the cache.                                                                                                                                                                                                                                                                                                                         |
| Do not point cargo at a shared external `target/`                           | `target` is mbx's managed-target link for the checkout; a `CARGO_TARGET_DIR`, `build.target-dir` or `build.build-dir` leaves the target unmanaged, and job admission refuses it.                                                                                                                                                                                                                                                                                                                                                                                                    |
| `-C target-cpu=native` ties the cache to one CPU class                      | Fine for a single host; it means the store cannot be shared with a different machine generation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

### The same rules for other toolchains

Ask three questions of any build cache before sharing it between workspaces:

1. Is the key derived from content rather than from where the files live?
2. Is a reused artifact still correct at a different path?
3. Does one process own the store and its size cap?

| Toolchain           | How it lands                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Go                  | `GOCACHE` and `GOMODCACHE` are content-addressed, so they share safely. Build with `-trimpath` for path-neutral binaries. In a Go project (`go.mod` or `go.work`) every sandboxed job names both at the host's own Go defaults: `~/go/pkg/mod` and the user cache directory's `go-build` (`~/Library/Caches/go-build`).                                                                                                                     |
| TypeScript via ttsc | `TTSC_CACHE_DIR` holds content-keyed plugin binaries, outside `node_modules` so installs stay lean. Cowshed has no ttsc capability: point it at `$HOME/.cache/ttsc-plugins` and declare `[caches] home = [".cache/ttsc-plugins"]` in main's `.cowshed.toml` to share it from your HOME, or keep it inside the checkout (`.cache/ttsc-plugins`, the managed shell's default), where every clone inherits main's warm copy through the image. |
| Bun                 | Shared: the isolated linker writes `node_modules/.bun` as links into the install cache, so every checkout reaches it through `~/.bun/install/cache`.                                                                                                                                                                                                                                                                                        |
| Python via uv       | The cache is shared through `~/.cache/uv`, like Bun's. A clone enters with the environment it copied only if that environment is free of its path — relocatable scripts, editable installs relative to site-packages, an interpreter whose path names no checkout.                                                                                                                                                                          |
| Nix                 | Content-addressed by definition; the store is shared and read-only to workspaces.                                                                                                                                                                                                                                                                                                                                                           |
| Zig, Gradle         | Shared at the tools' own defaults, `~/.cache/zig` and `~/.gradle/caches`; the same three questions apply.                                                                                                                                                                                                                                                                                                                                   |
| Nx                  | An Nx project (`nx.json`) has one `.nx/cache`, one `.nx/workspace-data` and one daemon/socket namespace across host, read-write and source-read-only jobs. Both state directories are fixed links into its private build volume. Caller Nx directory overrides never pass; inherited daemon records are deleted at fork.                                                                                                                    |
| C and C++ via cc-rs | Cowshed sets no compiler wrapper. A build script whose inputs name an absolute include or SDK path is per-checkout; keep them below the build directory.                                                                                                                                                                                                                                                                                    |

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
