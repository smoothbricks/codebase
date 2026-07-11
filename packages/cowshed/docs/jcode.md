# jcode integration (cowshed-core Rust API)

jcode consumes cowshed as a crate — no CLI subprocesses on the hot path. `cowshed-core` replaces two subsystems:
`.jcode-worktrees` linked-worktree management, and the hand-rolled parts of `jcode-base::sandbox` that enumerate cache
paths and worktree admin dirs.

```toml
# Cargo.toml
[dependencies]
cowshed-core = { path = "../smoothbricks/packages/cowshed/crates/cowshed-core" }
```

## Why this replaces linked worktrees

A cowshed workspace is a _standalone_ repository inside its own volume — the `.git` arrives complete via copy-on-write
when the image is cloned from main. Compared to `git worktree add`:

- No `.git/worktrees/<registration>` admin dirs in the main repo, so the Seatbelt profile no longer needs
  `push_git_worktree_metadata_grants`-style holes reaching back into the host checkout. The sandbox boundary is the
  volume boundary.
- No registration cleanup on session death; `rm` of one image file is complete teardown.
- No recursive `.jcode-worktrees/session_*/session_*` nesting — swarm workers fork _images_, not directory trees, so
  worker-of-worker costs one file too.
- Sync-back is `Workspace::push` — implemented as a _host-side fetch from the workspace mount_, so no agent-controlled
  git config or hook ever executes outside the sandbox. Remote pushes (origin, GitHub) stay coordinator work with the
  host's own git setup.

## Core types

```rust
use cowshed_core::{Cowshed, Coordinator, WorkspaceHandle, GrantDelta, EgressRule,
                   ExecRequest, CreateOptions};

// Discover the project from any path inside it (convention: git root = project).
let project = Cowshed::open("/Users/danny/Dev/conloca").await?;

// The coordinator is the sole mutating authority: create/fork/grant/revoke/land/destroy.
let coord: Coordinator = project.coordinator()?;

// One workspace per swarm worker. Clones main's live image: ~ms + attach.
let ws = coord.create("session_raven", CreateOptions::default()).await?;
assert_eq!(ws.branch(), "cowshed/session_raven");

// Hand the subagent a capability scoped to exactly its workspace — it can run,
// observe, checkpoint, and push, but physically cannot grant, destroy, or reach siblings.
let worker: WorkspaceHandle = coord.worker("session_raven")?;

// Sandboxed execution. The command runs under a launch-time Seatbelt profile
// generated from the workspace's *current* grant snapshot.
let run = worker.exec(ExecRequest::new(["cargo", "test", "-p", "jcode-core"])
    .cwd_rel("crates/jcode-core")
    .env("RUST_BACKTRACE", "1")).await?;
match run.outcome {
    cowshed_core::Outcome::Exit(code) => { /* child's code, passed through untouched */ }
    cowshed_core::Outcome::SandboxDenied(denial) => {
        // Present ONLY when authoritative evidence exists: the gateway logged the
        // egress denial, or kernel sandbox telemetry named the blocked operation.
        // denial.kind: FileWrite(path) | FileRead(path) | Egress(host)
        // denial.suggested_grant: ready to surface to the coordinator for approval
    }
}

// Ship and destroy (push = host-side fetch from the mount; see above).
worker.push(Default::default()).await?;
coord.destroy("session_raven", cowshed_core::Destroy::IfPushed).await?;
```

Handles are stateless views — they derive everything from the image, the mount table, and the marker. Re-obtaining one
after a crash is `project.workspace("session_raven")?`; there is no registry to repair.

## Layered grants (grant_write_paths, generalized)

jcode's swarm coordinator can widen a _running_ worker's sandbox — the equivalent of today's `grant_write_paths`
updating the session-owned `ToolContext`. In cowshed the grant set is a first-class, persisted object:

```rust
// Coordinator side: widen a worker mid-flight. One delta, applied atomically
// (compare-and-swap on the grant file's revision; a concurrent update is a
// Conflict, never a lost write). Workers can read their grants, never mutate.
coord.grant("session_raven", GrantDelta {
    write:  vec!["/Users/danny/Dev/shared-assets".into()],
    read:   vec!["/Users/danny/Dev/reference-corpus".into()],
    egress: vec![EgressRule::host("api.github.com")],
}).await?;
```

Semantics, matching what `sandbox.rs` documents today but enforced structurally:

- **Per-exec snapshot.** Every `exec` renders its Seatbelt profile from the grant set _as of that exec_ — exactly like
  jcode passing `additional_writable_roots` on every command because a worker process may predate the grant. Running
  processes keep their launch profile; the next exec sees the widened one.
- **Egress is immediate.** Network grants are enforced by the gateway per workspace identity, so they apply to
  already-running processes without relaunch.
- **Grants live outside the volume** (`<image>.grants.json`). A sandboxed worker cannot write its own grant file — the
  deny rules don't even need to mention it; it simply isn't inside any writable root. Only the coordinator (unsandboxed,
  or a differently-sandboxed supervisor) holds the mutating handle; a `WorkspaceHandle` can only _read_ its grant set.
- **Start closed.** A fresh workspace has an empty grant set; the closed baseline (own volume + designated cache
  subtrees + temp + its own gateway listener) is not expressed as grants and cannot be revoked away — it is the floor,
  so a buggy coordinator cannot brick a worker below "can build".
- **Revocation** (`coord.revoke(ws, delta)`, `--all` to clear) narrows the same way: next exec for filesystem,
  immediately for egress.

## What jcode deletes

| Today (jcode-base)                                                                                                       | With cowshed-core                                                                                                                                                                    |
| ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `wrap_supervisor_launch` + hand-enumerated cache write roots (`.cargo`, `.bun`, `Mozilla.sccache`, `.gradle`, zig dirs…) | `exec` / `sandbox_spec()` — writable roots are the workspace volume plus the designated layer-3 cache subtrees on `~/.cowshed/caches` (the gateway's mirror subtree stays read-only) |
| `push_git_worktree_metadata_grants`                                                                                      | gone — standalone repo per volume                                                                                                                                                    |
| `SCCACHE_NO_DAEMON=1` workaround comment                                                                                 | still `SCCACHE_NO_DAEMON=1`, now carried by the in-image cargo `[env]` config; sccache's default dir is relocated onto the shared cache volume, warm across all workers and main     |
| `command_mentions_sensitive_path` string scanning as a secrets backstop                                                  | secrets aren't reachable: not in the workspace, not in writable roots, tokens live in the gateway                                                                                    |
| `.envrc` compile/link env allowlist forwarding                                                                           | `ws.env()` returns the same wiring `cowshed ensure --envrc` emits; pass it to your supervisor's environment                                                                          |
| `.jcode-worktrees/` per-session trees (≈97% node_modules inodes)                                                         | one `.asif` file per worker                                                                                                                                                          |

The `RunSandboxMode::ReadOnly` narrowing survives as `ExecRequest::read_only()`, which drops the workspace volume from
the writable set (caches and temp remain) — useful for inspector-style tooling, mirroring `generate_inspector_profile`.

## Swarm shapes that become cheap

- **Fork-of-worker:** `ws.fork("session_raven_2")?` clones a mid-flight worker in milliseconds — divergent attempts from
  an identical warm state, each with a closed sandbox.
- **Checkpoint-retry:** `ws.checkpoint("before-migration")?` / `ws.restore("before-migration")?` gives workers
  transactional retries without re-cloning from main.
- **Coordinator-owned lifecycle:** because state is derived from disk, a crashed coordinator re-lists workers with
  `cowshed.workspaces()?` and resumes — no reconciliation pass, no stale registry.

## Environment wiring

`sandbox_spec().env` returns the exact (deliberately tiny) map `cowshed ensure --envrc` emits: at most
`COWSHED_GATEWAY_TOKEN` (only until its file-based verification lands — the bun cache export is already retired,
bunfig's relative cache dir is verified), `PORT`/`COWSHED_PORT_BASE` for dev servers, and non-load-bearing `COWSHED_*`
identity. Everything else is carried by in-image config files (bunfig, `.cargo/config.toml`) and the one-time host
relocation of the read-at-build cache dirs — wiring that survives processes cowshed never spawned. Merge the map into
the supervisor's child environment instead of maintaining the compile/link allowlist by hand; it contains no secrets by
construction, so the filtering problem the allowlist solved no longer exists.

## NAPI (TypeScript harnesses)

`cowshed-napi` (`@smoothbricks/cowshed`) exposes the same surface, async throughout:

```ts
import { openProject, connectWorkspace } from '@smoothbricks/cowshed';

const project = await openProject('/Users/danny/Dev/conloca');
const coord = await project.coordinator(); // full authority
const ws = await coord.createWorkspace(`task-${id}`);

// Subagents connect with the in-volume workspace token -> non-escalating handle.
const worker = await connectWorkspace(wsToken);
const run = await worker.exec(['bun', 'test'], { cwdRel: 'packages/lmao' });
// sandboxDenied is present only when authoritative evidence exists (gateway log,
// kernel sandbox telemetry); otherwise run.exitCode is the child's, untouched.
if (run.sandboxDenied) coordinator.requestGrant(ws.name, run.sandboxDenied.suggestedGrant);

await worker.push();
await coord.destroyWorkspace(ws.name, { ifPushed: true });
```

The CLI remains the integration point for agents that only have a shell; CLI, NAPI, and the Rust API are thin layers
over the same cowshed-core, so behavior (grants, exit semantics, stderr narration) is identical everywhere.
