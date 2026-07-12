# cowshed — Warm Git Workspaces

cowshed provides instant, isolated, copy-on-write git workspaces on macOS and Linux. A workspace is one copy-on-write
clone — an APFS disk image on macOS, a ZFS dataset on Linux — cloned in milliseconds, deleted as one operation, with
everything inside it (source, `.git`, `node_modules`, build state) invisible to the host filesystem's inode namespace.
Commands inside a workspace run under a layered, deny-default sandbox whose filesystem and egress capabilities start
closed and widen only through explicit, controller-owned grants.

## Platforms

| Platform | Substrate                              | Sandbox                      | Secrets                        |
| -------- | -------------------------------------- | ---------------------------- | ------------------------------ |
| macOS    | APFS images (`.asif` / `.sparseimage`) | Seatbelt (`sandbox-exec`)    | Keychain                       |
| Linux    | ZFS datasets (snapshot + clone)        | Landlock + network namespace | secret-service / systemd-creds |

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
- Convention over configuration: remote discovery proposes candidates; one explicit, stable `repo_id` binding is the
  only required identity decision, and an otherwise empty `.cowshed.toml` is expected.
- Self-driving CLI: machine-readable stdout, agent guidance on stderr.

## Non-goals

- Kernel-grade isolation. Seatbelt/Landlock confine file writes and egress on a shared kernel; cowshed is not a defense
  against hostile kernel exploits. VM-class isolation is out of scope.
- Backup of workspace images. Durability is git: work leaves a workspace via `cowshed push`. (On ZFS, `zfs send -i`
  additionally enables real incremental backup of main — 09_substrates.md.)
- Package-manager replacement. cowshed redirects existing tools (bun, cargo, Go) via standard configuration; it does not
  reimplement them.

## Architecture

```
                                  ┌────────────────────────────┐
   Rust consumers ─── cowshed-core ────┤                            │
   Agent harnesses ─── cowshed-cli ─────┤         cowshed-core           │
   Bun/Node ──────── cowshed-napi ────┤  images · mounts · marker  │
                                  │  grants · seatbelt · env   │
                                  └──────┬──────────┬──────────┘
                                         │          │
                    ┌────────────────────┘          └──────────────────┐
                    ▼                                                  ▼
        ~/.cowshed/  (= the cowshed.store volume)                  cowshed-gateway (localhost)
        <owner>/<repo>/main{.asif|.sparseimage} ─ clonefile ─►    npm/cargo mirror · per-workspace CA interception
        <owner>/<repo>/sessions/<ws>{.asif|.sparseimage}          repo-mirror verb · Arrow audit (13)
        <owner>/<repo>/sessions/<ws>{.asif|.sparseimage}.grants.json    │
                    │                                                ▼
                    ▼ format-selected attach                 cowshed.caches APFS volume
        ~/.cowshed/mnt/<owner>/<repo>/<ws>/          ◄─ sccache/zig/gradle ─  ~/.cowshed/caches/
        (workspace: src + .git + node_modules + target)
```

The diagram shows the macOS/APFS substrate: ASIF images use `.asif` and `diskutil image attach`, while SPARSE fallback
images use `.sparseimage` and `hdiutil attach`; detached metadata selects the tool and must agree with the extension
(01_storage.md). On Linux the same logical shape holds with ZFS datasets in place of image files and the store mounted
directly at `~/.cowshed` with the caches dataset nested at `~/.cowshed/caches` (09_substrates.md). Both volumes are
dedicated: the Data volume carries no cowshed bytes (01_storage.md).

Gateway reachability is deliberately platform-specific. macOS package clients use the workspace's `portBlock.base`.
Linux allocates no `portBlock`: each attached workspace has a private loopback/netns and a controller-launched trusted
connector at `127.0.0.1:7644`; that connector relays only to the workspace's bind-mounted per-workspace Unix gateway
socket. Thus ordinary Bun, Cargo, Go, and proxy-aware clients keep standard localhost HTTP URLs while the socket inode
plus network namespace remains the primary Linux workspace identity (04_sandbox.md/05_gateway.md).

**Git never crosses the network from inside a workspace.** A workspace's git speaks only to local paths: the `host`
remote (main's mount, fetch-only for rebase) and read-only bare mirrors on the cache volume. Pulling a new upstream repo
is a gateway _control-plane_ verb (`cowshed repo mirror`, 06_cli.md/05_gateway.md), never in-sandbox git-over-HTTPS;
publishing to a real origin is coordinator work, host-side, outside any sandbox (02_workspaces.md). The gateway box
therefore carries npm/cargo mirrors, **per-workspace-CA TLS interception** as the default for granted egress hosts (with
an `--opaque` CONNECT fallback for pinned clients — 05_gateway.md), and the repo-mirror verb — no git credential broker.
The workspace holds only public trust anchors; the CA private key stays with the gateway.

The `<owner>/<repo>` pair is the primary project `repo_id`: lowercase `owner/repo` normalized from an explicitly
selected remote URL, stable across machines and checkout moves. The controller records that repository binding,
validates it on every open, permits multiple bound identities with exactly one primary, and requires an explicit
`repo_id` for a local-only repository. Discovery may propose candidates but never silently mints or selects one. Each
component is validated and encoded independently before path joining. Trusted project policy lives only at
`~/.cowshed/<owner>/<repo>/policy.json`, outside every workspace and denied to sandboxes (01_storage.md).

State is derived, never stored: the workspace clones are the registry (readdir / `zfs list`), the kernel mount table is
the attachment state (getmntinfo), the controller-owned remote binding establishes repository identity, and an
in-workspace marker (`.cowshed/workspace.json`) identifies a workspace incarnation. There is **no mutable state
database**. The persistent daemons are the gateway (one per host) and the per-workspace shell supervisors (11_shell.md),
with the optional MCP socket server (12_mcp.md). Linux additionally has one ephemeral minimal connector per attached
workspace; it is attachment plumbing and stores no authority. None of these processes _store_ authoritative state — kill
any of them and the next command rederives everything from disk and the mount table.

Observability is **distributed tracing into Arrow columns** (13_telemetry.md): lifecycle ops, jobs, and gateway requests
are spans propagating W3C trace context, flushed as queryable Arrow segments on the store volume via **lmao**
(`packages/lmao-rs`). There is no telemetry daemon and no on-disk text log — the audit trail, the perf SLOs, and the
sandbox trace-assertions are all queries over one substrate.

## Crate map

| Crate                  | Kind          | Responsibility                                                                                         |
| ---------------------- | ------------- | ------------------------------------------------------------------------------------------------------ |
| `cowshed-core`         | lib           | Image/volume lifecycle, clonefile, marker & grant files, Seatbelt profile generation, env wiring, exec |
| `cowshed-cli`          | bin `cowshed` | Self-driving CLI over cowshed-core; stdout machine-readable, stderr guidance                           |
| `cowshed-gateway`      | bin + lib     | Localhost registry mirror, repo-mirror verb, CONNECT tunnel, audit                                     |
| `cowshed-napi`         | cdylib        | napi-rs bindings exposing cowshed-core to Bun/Node                                                     |
| `cowshed-escape-tests` | lib (tests)   | Sandbox escape regression harness                                                                      |

## Consumers

- **Rust consumers** link `cowshed-core` directly. A coordinator holds a `Coordinator` (07_api.md), creates workspaces
  for workers, hands each worker a non-escalating `WorkspaceHandle`, and widens grants mid-session through
  `Coordinator::grant`.
- **Agent harnesses** use the CLI: `cowshed new <name>` prints a mount path on stdout and next-step guidance on stderr —
  a "warm worktree" with no further setup.
- **Bun/Node tooling** uses `@smoothbricks/cowshed` (cowshed-napi) for programmatic control.
- **CI runners.** A Linux+ZFS host runs each GitHub Actions job in an ephemeral, sandbox-confined clone of a warm
  headless main — warm caches, no on-runner Nix install, and registry/git secrets held host-side in the gateway rather
  than in GitHub (10_ci.md).

## Tradeoffs

**VMs / Apple containers rejected as substrate.** Apple's Containerization framework runs Linux guests (excludes
Swift/Xcode/iOS work), and Virtualization.framework enforces a two-concurrent-VM cap for macOS guests — unusable for a
fleet. Seatbelt-on-host covers all toolchains at zero boot cost; the reduced isolation ceiling is documented and
accepted. The remaining unsandboxed surfaces — main and the controller tooling — can additionally be placed behind a
kernel uid boundary by running the whole stack as a dedicated `dev` account (deployment posture B, 14_nix.md); that is
the recommended hardening, not a substrate change.

**Warm pools rejected.** Pre-attached image pools would hide attach latency (~235 ms) but add a claim/refill state
machine and staleness handling. The cold path already meets the ≤ 1 s budget; the acceptable-latency envelope (10 s for
humans, minutes for agents) makes pooling pure complexity.
