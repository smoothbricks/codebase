# cowshed — Warm Git Workspaces

cowshed provides instant, isolated, copy-on-write git workspaces on macOS and Linux. A workspace is one copy-on-write
clone — an APFS disk image on macOS, a ZFS dataset on Linux — cloned in milliseconds, deleted as one operation, with
everything inside it (source, `.git`, `node_modules`, build state) invisible to the host filesystem's inode namespace.
Commands inside a workspace run under a layered, deny-default sandbox whose filesystem and egress capabilities start
closed and widen only through explicit, controller-owned grants.

## Platforms

| Platform | Substrate                       | Sandbox                      | Secrets                        |
| -------- | ------------------------------- | ---------------------------- | ------------------------------ |
| macOS    | APFS images (`.asif`, hdiutil)  | Seatbelt (`sandbox-exec`)    | Keychain                       |
| Linux    | ZFS datasets (snapshot + clone) | Landlock + network namespace | secret-service / systemd-creds |

The substrate and enforcement layers sit behind traits (09_substrates.md, 04_sandbox.md); every concept above them —
grants, gateway, cache taxonomy, CLI contract, marker files — is identical across platforms.

## Problem

Agent-driven development multiplies workspaces. Three failure modes follow:

1. **Inode explosion.** Each conventional worktree materializes its own `node_modules`, `target/`, and build caches —
   hundreds of thousands of host inodes per session, millions per fleet. APFS maintenance (`fsck_apfs`, snapshot
   deletion, Spotlight) scales with object count and becomes intractable on the volume that also holds the user's data.
2. **Worktree coupling.** Linked git worktrees keep back-references into the primary repository's `.git/worktrees/<id>`.
   Sandboxes must punch holes to those paths, deletion leaves stale registrations, and a cloned linked worktree is
   invalid by construction.
3. **Sandbox cache leaks.** Sharing toolchain caches by granting write access to a dozen `$HOME`-scattered directories
   gives every sandboxed process write access to state that every other build consumes — a poisoning surface and an
   inode factory at once.

## Goals

- Workspace creation ≤ 1 s cold; deletion perceived-instant; zero per-workspace host-inode growth beyond O(1) files.
- Full isolation: standalone `.git` per workspace, no references into the host checkout.
- Warm by construction: every workspace starts as a clone of the main workspace's live state — dependencies installed,
  build caches hot.
- Layered sandboxing: start closed (own mount + designated cache subtrees + temp; localhost-only egress), widen at
  runtime via grants, narrow via revocation.
- Secrets never inside a workspace: credentials live only in the gateway (Keychain-backed).
- Shared caches without duplicate downloads or duplicate storage, using clonefile wherever physically possible.
- Convention over configuration: zero required config; an empty `.cowshed.toml` is the expected state.
- Self-driving CLI: machine-readable stdout, agent guidance on stderr.

## Non-goals

- Kernel-grade isolation. Seatbelt/Landlock confine file writes and egress on a shared kernel; cowshed is not a defense
  against hostile kernel exploits. VM-class isolation is out of scope.
- Backup of workspace images. Durability is git: work leaves a workspace via `cowshed push`. (On ZFS, `zfs send -i`
  additionally enables real incremental backup of main — 09_substrates.md.)
- Package-manager replacement. cowshed redirects existing tools (bun, cargo, git) via standard configuration; it does
  not reimplement them.

## Architecture

```
                                  ┌────────────────────────────┐
   jcode (Rust) ──── cowshed-core ────┤                            │
   Claude Code ───── cowshed-cli ─────┤         cowshed-core           │
   Bun/Node ──────── cowshed-napi ────┤  images · mounts · marker  │
                                  │  grants · seatbelt · env   │
                                  └──────┬──────────┬──────────┘
                                         │          │
                    ┌────────────────────┘          └──────────────────┐
                    ▼                                                  ▼
        ~/.cowshed/store/  (cowshed.store volume)                  cowshed-gateway (localhost)
        <project>/main.asif          ── clonefile ──►          npm/cargo mirror · CONNECT
        <project>/sessions/<ws>.asif                           repo-mirror verb · audit log
        <project>/sessions/<ws>.asif.grants.json                     │
                    │                                                ▼
                    ▼ hdiutil attach (nobrowse)               cowshed.caches APFS volume
        ~/.cowshed/mnt/<project>/<ws>/          ◄─ sccache/zig/gradle ─  ~/.cowshed/caches/
        (workspace: src + .git + node_modules + target)
```

The diagram shows the macOS/APFS substrate; on Linux the same shape holds with ZFS datasets in place of `.asif` files
and `<pool>/cowshed/{store,caches}` datasets behind the same `~/.cowshed/{store,caches}` mountpoints (09_substrates.md).
Both volumes are dedicated: the Data volume carries no cowshed bytes (01_storage.md).

**Git never crosses the network from inside a workspace.** A workspace's git speaks only to local paths: the `host`
remote (main's mount, fetch-only for rebase) and read-only bare mirrors on the cache volume. Pulling a new upstream repo
is a gateway _control-plane_ verb (`cowshed repo mirror`, 06_cli.md/05_gateway.md), never in-sandbox git-over-HTTPS;
publishing to a real origin is coordinator work, host-side, outside any sandbox (02_workspaces.md). The gateway box
therefore carries npm/cargo mirrors, an allowlisted CONNECT tunnel, and the repo-mirror verb — no git credential broker.

State is derived, never stored: the workspace clones are the registry (readdir / `zfs list`), the kernel mount table is
the attachment state (getmntinfo), and an in-workspace marker (`.cowshed/workspace.json`) is the identity. There is **no
state database**. The only daemons are the gateway (one per host) and the per-workspace shell supervisors (11_shell.md)
with the optional MCP socket server (12_mcp.md); none of them _store_ authoritative state — kill any of them and the
next command rederives everything from disk and the mount table.

## Crate map

| Crate                  | Kind          | Responsibility                                                                                         |
| ---------------------- | ------------- | ------------------------------------------------------------------------------------------------------ |
| `cowshed-core`         | lib           | Image/volume lifecycle, clonefile, marker & grant files, Seatbelt profile generation, env wiring, exec |
| `cowshed-cli`          | bin `cowshed` | Self-driving CLI over cowshed-core; stdout machine-readable, stderr guidance                           |
| `cowshed-gateway`      | bin + lib     | Localhost registry mirror, repo-mirror verb, CONNECT tunnel, audit                                     |
| `cowshed-napi`         | cdylib        | napi-rs bindings exposing cowshed-core to Bun/Node                                                     |
| `cowshed-escape-tests` | lib (tests)   | Sandbox escape regression harness                                                                      |

## Consumers

- **jcode** links `cowshed-core` directly. Its swarm coordinator holds a `Coordinator` (07_api.md), creates workspaces
  for workers, hands each worker a non-escalating `WorkspaceHandle`, and widens grants mid-session exactly as it does
  today with `grant_write_paths` — but through `Coordinator::grant` instead of bespoke Seatbelt plumbing.
- **Claude Code** (and any agent harness) uses the CLI: `cowshed new <name>` prints a mount path on stdout and next-step
  guidance on stderr — a "warm worktree" with no further setup.
- **Bun/Node tooling** uses `@smoothbricks/cowshed` (cowshed-napi) for programmatic control.
- **CI runners.** A Linux+ZFS host runs each GitHub Actions job in an ephemeral, sandbox-confined clone of a warm
  headless main — warm caches, no on-runner Nix install, and registry/git secrets held host-side in the gateway rather
  than in GitHub (10_ci.md).

## Tradeoffs

**VMs / Apple containers rejected as substrate.** Apple's Containerization framework runs Linux guests (excludes
Swift/Xcode/iOS work), and Virtualization.framework enforces a two-concurrent-VM cap for macOS guests — unusable for a
fleet. Seatbelt-on-host covers all toolchains at zero boot cost; the reduced isolation ceiling is documented and
accepted.

**Warm pools rejected.** Pre-attached image pools would hide attach latency (~235 ms) but add a claim/refill state
machine and staleness handling. The cold path already meets the ≤ 1 s budget; the acceptable-latency envelope (10 s for
humans, minutes for agents) makes pooling pure complexity.
