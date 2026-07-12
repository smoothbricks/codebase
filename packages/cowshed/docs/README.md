# cowshed — warm git workspaces

cowshed gives you **instant, isolated, warm workspaces** for any git repository. A cowshed workspace is a full
standalone checkout — source, `.git`, `node_modules`, `target/`, every build cache — cloned copy-on-write from your live
main workspace in milliseconds. Work in it, run agents in it, destroy it. The host filesystem gains one lightweight
object per workspace instead of a hundred thousand inodes.

## Platforms

cowshed runs on two copy-on-write substrates, auto-detected from the filesystem your project root sits on; the commands,
sandbox, gateway, and cache model are identical across them.

| Platform | Substrate | A workspace is                                                                                                                | Sandbox                        |
| -------- | --------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| macOS    | APFS      | a sparse disk image (ASIF; clonefile + `diskutil image attach`, `hdiutil` for the SPARSE fallback), mounted as its own volume | Seatbelt (`sandbox-exec`)      |
| Linux    | ZFS       | a dataset clone off a `main@cowshed:<name>` snapshot (no attach, no fsck)                                                     | Landlock + loopback-only netns |

On ZFS everything is faster — `cowshed new` is tens of milliseconds and checkpoints are literal snapshots — see
[zfs.md](zfs.md). A third consumer, **self-hosted CI runners** ([ci.md](ci.md)), uses cowshed on Linux/ZFS to give every
GitHub Actions job a warm, sandboxed, ephemeral workspace. The examples below are macOS; on Linux the same `~/.cowshed`
and `~/.cowshed/caches` paths are backed by ZFS datasets, with secret-service in place of Keychain.

```
$ cowshed new raven
cowshed: cloned acme/widget main image (copy-on-write)
cowshed: attached at ~/.cowshed/mnt/acme/widget/raven
cowshed: branch cowshed/raven created from main @ 6f3a2c1
next: cowshed shell raven
next: cowshed exec raven -- bun test
~/.cowshed/mnt/acme/widget/raven
```

Everything above the last line is stderr guidance; the last line — the workspace path — is the only thing on stdout.
That split holds for every command: **stdout is for machines, stderr is for you (and for agents reading hints).**

## Why

- **Instant.** `cowshed new` is an APFS clonefile (~2 ms) plus a volume attach (~250 ms). `cowshed rm` detaches and
  deletes one file — no `rm -rf` of 90k dependency files.
- **Warm.** Workspaces clone your _live main workspace_, so they start with materialized `node_modules`, a hot
  `target/`, warm package caches. There are no templates to configure or refresh: main is the base, by convention.
- **Isolated.** Each workspace is an independent APFS volume or ZFS dataset with a standalone `.git` (no linked worktree
  registrations). Sandboxed commands can write only to their own workspace, the shared cache volume, and temp. Egress is
  gateway-only: macOS uses its assigned localhost block base; Linux uses a trusted connector at private-netns
  `127.0.0.1:7644` which relays only to that workspace's mounted Unix gateway socket. The gateway holds all credentials.
  Workspaces contain **no secrets and no `.env` files**.
- **Layered.** Sandboxes start closed and are widened per-workspace at runtime:
  `cowshed grant raven --write <path> --egress <domain>`.
- **Inode-friendly.** Millions of dependency/build inodes move off your Data volume into image files, so Spotlight,
  snapshots, and `fsck_apfs` work again.

## Five-minute quickstart

```sh
# 1. One-time: convert your checkout into an image-backed main workspace.
#    Same path before and after; this copies the tree once (minutes for big repos).
cd <project-root>
cowshed adopt --remote origin

# 2. Start the gateway (once per login; see docs/gateway.md for launchd setup).
cowshed gateway run &

# 3. Create a warm workspace. Prints its path on stdout.
cowshed new raven

# 4. Work in it — interactively, or sandboxed:
cowshed shell raven
cowshed exec raven -- bun test

# 5. Ship the branch back to main's repo, then destroy the workspace.
cowshed push raven
cowshed rm raven
```

Add this to your repo's `.envrc` so every workspace (including main) self-heals and gets identical cache/gateway wiring:

```sh
eval "$(cowshed ensure --envrc)"
```

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

Before adoption, cowshed proposes `repo_id` candidates from configured remotes; you select one remote (for example
`origin`). Its URL is normalized to stable lowercase `owner/repo`, recorded, and revalidated whenever the project opens.
Moving or recloning the checkout does not change the identity. Multiple bindings are allowed but one is primary. cowshed
never silently mints or selects an identity; use `--repo-id owner/repo` for a repository with no usable remote.

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
