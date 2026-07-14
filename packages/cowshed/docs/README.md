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
{"ok":true,"result":{"workspace":"raven","mount":"/Users/me/.cowshed/mnt/acme/api/raven","baseCommit":"6f3a2c1000000000000000000000000000000000"}}
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

# 5. Preserve the branch, then remove the workspace.
cowshed push "$WS"
cowshed rm "$WS"
```

From outside the repository, make selection explicit:

```sh
cowshed new raven --project <project-root>
```

For the complete repository-selection model, multi-repository examples, agent loop, JSON contract, and safe cleanup
rules, start with [usage.md](usage.md).

## Where things live

| What                                                                | Where                                                                        |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Images (main + workspaces + checkpoints)                            | `~/.cowshed/<owner>/<repo>/` (the primary, component-safe `repo_id`)         |
| Workspace mounts                                                    | `~/.cowshed/mnt/<owner>/<repo>/<workspace>`                                  |
| Adopted main mount                                                  | its original `<project-root>`                                                |
| Trusted project policy                                              | `~/.cowshed/<owner>/<repo>/policy.json` (controller-owned, sandbox-denied)   |
| Repository binding                                                  | `~/.cowshed/<owner>/<repo>/repository.json`                                  |
| Shared writable build caches (Cargo, sccache, zig, Gradle, Go, Nix) | exact tool subdirectories under `~/.cowshed/caches`                          |
| Gateway registry and repository mirrors                             | `~/.cowshed/caches/{mirror,repo-mirrors}` (gateway-owned, sandbox-read-only) |

Before adoption, cowshed derives a stable lowercase `owner/repo` identity from configured remotes when the choice is
unambiguous. The binding is recorded and revalidated whenever the project opens. Moving the checkout does not change the
identity. Use `--repo-id owner/repo` when a local-only repository or ambiguous remote set cannot supply one.

The `owner` and `repo` components are validated and encoded independently; cowshed never treats a remote string as a
filesystem path. `policy.json` is trusted host policy, not repository content, and is never read from a workspace.

There is no mutable state database. Images/datasets, repository bindings, the kernel mount table, and in-image markers
_are_ the state; every command derives the world by looking at them.

## The cache model in one paragraph

Downloads happen once, ever: the gateway mirrors npm, cargo, and Go module registries (and, via `cowshed repo`, git
repositories) and caches artifacts on `~/.cowshed/caches`. On macOS each workspace's clients use its own localhost
`portBlock.base`; on Linux, where no port block exists, ordinary Bun/Cargo/Go and proxy-aware clients use
`http://127.0.0.1:7644` inside their private netns. A trusted per-workspace connector forwards those bytes only to the
mounted per-workspace Unix gateway socket, which remains the primary endpoint identity. Bun's install cache lives
_inside_ each workspace image — inherited from main via copy-on-write — because bun clones out of it, and clonefile
can't cross volumes; that keeps `bun install` on its fast path. Read-at-build caches are shared under
`~/.cowshed/caches`: Cargo uses distinct writable `cargo/{registry,git}` directories; Go uses `go/{mod,build}`; Nix uses
`nix/{cache,state}`; sccache, zig, and Gradle have named roots. Gateway artifacts are not Cargo caches: registry objects
live under `mirror/` and bare repository mirrors under `repo-mirrors/`, both gateway-owned and read-only to workspaces.
On declarative hosts, the system/home-manager module owns all relocations, including `~/.cache/nix → nix/cache` and
`~/.local/state/nix → nix/state`; cowshed only validates them. `cowshed adopt --imperative-host-setup` is an explicit
exception for non-declarative hosts, never an automatic fallback after declarative validation fails.

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
