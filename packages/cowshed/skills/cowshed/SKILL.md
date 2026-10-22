---
name: cowshed
description:
  Give each agent, fork, or subtask an instant warm copy-on-write clone of a repository instead of a cold git worktree.
  Use when spawning parallel agents that each need their own checkout, when a worktree would force a multi-minute cold
  rebuild, when isolating risky or destructive work, or when running any cowshed command (adopt, setup, fork, path,
  exec, grant, land, push, rm, gc, rebase, doctor).
---

# cowshed — warm workspaces for parallel agents

A cowshed workspace is a full standalone checkout — source, `.git`, `node_modules`, `target/`, every build cache —
cloned copy-on-write from the repository's warm `main` image. A `git worktree` gives an agent a cold tree whose build
cache starts empty and a first build that costs minutes; a cowshed workspace starts warm. On a long-used repository
`cowshed fork` is dominated by two steps. The clone call returns in milliseconds, but the clone's first write, during
its attach or mount, makes APFS copy the extent map of main's image file: one loaded RAW observation took about 140 µs
per extent, and main's image fragments with every write it takes while clones share its blocks. Past 7,140 extents,
about a second at that rate, fork refuses and names `cowshed defrag main`. Then setup walks the whole tree for symlinks
that point outside it, about 5 s over a million entries. The number of attached images does not matter.

`fork [<src>] <ws>` is the only creation verb; the source defaults to main. Every fork carries source edits and
untracked files by default on its own `cowshed/<ws>` branch. `--clean` discards tracked edits and nonignored untracked
paths; `--ref <rev>` implies clean. Ignored dependencies and build state stay warm. `mv` is a rename, not a fresh mint.

## Use it

1. Adopt once: `cowshed adopt <path> --capacity <size>`. Adoption runs a secret gate first; `--quarantine` moves
   findings aside, a waiver in the named `waivers.json` is only for committed synthetic fixtures.
2. Host storage missing → `cowshed doctor` (bare doctor mutates nothing; `--repair` also runs the gc sweep), then its
   `next:` command, usually `cowshed setup`. Setup installs the invoking build as the one installed cowshed (CLI and
   daemon, one artifact): typing `cowshed setup` runs the installed copy and installs nothing new, so install a checkout
   build via its own `packages/cowshed/bin/cowshed setup`, never symlink `cowshed` on PATH into a checkout, and expect
   setup to refuse a build older than the installed one (`--downgrade` only when intended).
3. One workspace per agent; work there, never in shared `main`. Pass `--project <git-root>` explicitly rather than
   trusting cwd. Retire finished sheds; storage grows with divergence, not clone count.
4. Workspaces publish `.cowshed/env`; direnv users need nothing extra (`source_env_if_exists .cowshed/env` is the
   optional hook). After a reboot or eject, `cowshed attach` re-mounts; it is idempotent.

## Commands

| Task                    | Command                                                              | Use                                                                                                                                                                                                                                        |
| ----------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Create from main        | `cowshed fork <ws>`                                                  | Carry main's current state warm.                                                                                                                                                                                                           |
| Create from a sibling   | `cowshed fork <src> <ws>`                                            | Carry another workspace's current state warm.                                                                                                                                                                                              |
| Create at a stable slot | `cowshed fork <ws> --slot <n>`                                       | Recycle a stable path for path-keyed caches.                                                                                                                                                                                               |
| Create clean            | `cowshed fork [<src>] <ws> --clean`                                  | Reset tracked edits and remove nonignored untracked files.                                                                                                                                                                                 |
| Create at a ref         | `cowshed fork [<src>] <ws> --ref <rev>`                              | Clean and branch at the source's revision.                                                                                                                                                                                                 |
| Locate                  | `cowshed path <ws>`                                                  | Print the live mount path.                                                                                                                                                                                                                 |
| Run a command           | `cowshed exec <ws> -- <cmd>`                                         | Execute argv inside the workspace sandbox.                                                                                                                                                                                                 |
| Grant host paths        | `cowshed grant <ws> --read <path...> [--write <path...>]`            | Widen filesystem access from the next exec; omit flags to list.                                                                                                                                                                            |
| Grant network reach     | `cowshed grant <ws> --egress <host>`                                 | Admit one host through the gateway; separately audited.                                                                                                                                                                                    |
| Grant a pinned client   | `cowshed grant <ws> --egress <host> --opaque`                        | Tunnel without interception for a client that verifies the real certificate (Go on macOS).                                                                                                                                                 |
| Grant service ports     | `cowshed grant <ws> --ports <N>`                                     | macOS: grow the port block to at least N service ports before launching services.                                                                                                                                                          |
| Grant any loopback port | `cowshed grant <ws> --loopback-any`                                  | macOS: let every job connect to any loopback port, siblings' and the control plane's included; for a tool whose port-0 listener the preload cannot move. Listed and warned; `revoke --loopback-any` withdraws it.                          |
| List this project       | `cowshed ls`                                                         | Show its workspaces.                                                                                                                                                                                                                       |
| List every project      | `cowshed ls --all`                                                   | Show workspaces store-wide.                                                                                                                                                                                                                |
| Inspect host            | `cowshed doctor`                                                     | Check host and workspace invariants without mutation.                                                                                                                                                                                      |
| Inspect compile cache   | `cowshed sccache status`                                             | Check daemon health and cache hits before debugging misses.                                                                                                                                                                                |
| Attach or detach        | `cowshed attach <ws>` / `cowshed detach <ws>`                        | Mount or park one session workspace.                                                                                                                                                                                                       |
| Attach or detach all    | `cowshed attach --all` / `cowshed detach --all`                      | Mount or park all session workspaces.                                                                                                                                                                                                      |
| Checkpoint or restore   | `cowshed checkpoint <ws> <label>` / `cowshed restore <ws> <label>`   | Save or roll back an image.                                                                                                                                                                                                                |
| Rebase                  | `cowshed rebase <ws>`                                                | Rebase the workspace branch onto main.                                                                                                                                                                                                     |
| Land with a check       | `cowshed land <ws> --target main --check '<bare command>'`           | Check, fast-forward main, and retire on success.                                                                                                                                                                                           |
| Rebase within a lane    | `cowshed rebase <ws> --into <lane> --ungated`                        | Rebase onto the lane base's branch by Git alone: no task hashing, no carry.                                                                                                                                                                |
| Fold into a lane base   | `cowshed land <ws> --into <lane> --ungated`                          | Fast-forward the lane base by Git alone and retire; the lane base's own land into main gates. Main refuses it.                                                                                                                             |
| Deliver a branch        | `cowshed push <ws> --branch <name>`                                  | Put the workspace branch in main's repository for review.                                                                                                                                                                                  |
| Retire                  | `cowshed rm <ws>`                                                    | Remove a shed whose work main holds; a containment refusal names commits to fold into the batch.                                                                                                                                           |
| Reclaim                 | `cowshed gc --dry-run` / `cowshed gc`                                | Review, then reclaim orphaned storage.                                                                                                                                                                                                     |
| Grow an image           | `cowshed resize <ws> <size>`                                         | Grow an image; resize never shrinks it.                                                                                                                                                                                                    |
| Make clones cheap again | `cowshed defrag main`                                                | Rewrite main contiguously when doctor warns `main-extents`.                                                                                                                                                                                |
| Replace build state     | `cowshed reseed <ws> [--from <target>] [--dry-run]`                  | Discard live build contents for an existing seed clone (default main); source untouched, missing work rebuilds cold. Never refreshes a seed; absent seed refuses. Allocation counts are not physical free-space promises.                  |
| Clear Nx caches         | `cowshed cache clear [<ws>]`                                         | Delete the workspace's `.nx/cache` and `.nx/workspace-data` entries and the project's gateway Nx store, with entry counts; refuses, naming it, while a job, land or Nx request runs. `nx reset` only removes the links and clears nothing. |
| Rename or move          | `cowshed mv <ws> <new-name>` / `cowshed mv main <new-checkout-path>` | Rename a workspace or move the adopted checkout.                                                                                                                                                                                           |
| Hold a lock file        | `cowshed lock <path> [--state-fd <n>] [--owned-pid <pid>] -- <cmd>`  | Run a command holding a lock file's flock(2), which it inherits; on SIGUSR1 while queued, signals its inode's proven owner through an `--owned-pid` life captured at start, and then never runs the command.                               |
| See who holds a lock    | `cowshed lock-holders <path>`                                        | The kernel's account of a lock file as JSON: each descriptor's custody (`holds`, `open`, `recorded`, Darwin's `unexposed`); observes only.                                                                                                 |
| Hold a command's group  | `cowshed hold --control-fd <n> --state-fd <m> -- <cmd>`              | Run a command as an unreaped session leader cowshed stays outside of; `signal <SIG>` on `n` reaches its whole group while held; `release` reaps, EOF before it ends the group (TERM, grace, KILL); `HoldEvent` lines on `m`.               |
| Inspect tree holders    | `cowshed tree-holders <path>`                                        | Observe processes holding a working directory, root, executable, open file or mapping under a tree; one generated `TreeHolder` JSON value per line, with exact tagged path bytes. Never signals them.                                      |
| Inspect fd holders      | `cowshed fd-holders --device <0x…> \| --inode <n>`                   | Observe every process with a descriptor on a pipe, socket or file named as `hold`'s `started.output` names it; one generated `FdHolders` object: each holder's pid, life, descriptors, matches, and an `uninspected` count.                |

## Merge flow

1. Work and commit in the shed. Never reuse sheds across tasks.
2. `cowshed rebase <ws>` onto current main; resolve conflicts in the shed. Land will not do this for you.
3. Gate, then land — **but check for a repo land path first.** Repositories with their own land command require that
   route to rebase, gate on the _host_ (sandbox hashes differ and host-boundary tests cannot run sandboxed), and only
   then fast-forward. A bare `cowshed land` is refused when the repository requires its own route. Elsewhere:
   `cowshed land <ws> --check '<cmd>'` (repeatable; runs inside the sandbox after grant sync).
4. A successful land retires by default. If retire was refused after the fast-forward, main already moved: run
   `cowshed rm <ws>`, never land again. With `--no-retire` or after `push`, `rm` once main contains HEAD and every other
   local branch (a fork keeps only `cowshed/<ws>`; one branch per shed).
5. `--force` on `rm` waives only transient state (dirty tree, the workspace's own running jobs; never a process cowshed
   does not run holding the mount) and destroys the uncommitted tree; it can never waive containment. `--abandon` is the
   sole way to destroy unlanded commits (they are bundled to `sessions/.trash/` first). Never answer an `rm` refusal by
   adding a flag.
6. Batch work is lane work
   ([02_workspaces.md, "Lanes"](../../../../specs/cowshed/02_workspaces.md#lanes-fork-and---into)): a repository's next
   batch is a lane base, and a finished shed's work moves into it with `cowshed rebase <ws> --into <batch> --ungated`,
   then `cowshed land <ws> --into <batch> --ungated`: Git alone, no check, no build-volume carry or adoption. The land
   fast-forwards the batch shed and retires the shed at once, so it never waits for the batch to reach main. A unit
   meant for the batch from the start forks from it (`cowshed fork <batch> <unit>`); the batch itself lands to main as
   an ordinary gated workspace, and its `--check` is the lane's one build.
7. A shed whose work main already holds, or that carries none, is removed with plain `cowshed rm <ws>` and no check
   first: rm proves containment against live main, over every local branch: a commit is held when main's content carries
   its change or main holds a patch-equivalent commit (`git cherry`), so a cherry-pick main later edited past still
   counts. A containment refusal names the commits held by neither, and that shed is folded into the batch by step 6.
   Every other refusal (a workspace younger than the age floor, a dirty tree, a process holding the mount, a lifecycle
   operation in progress) names its own remedy.

## Enter a shed through `cowshed exec`

- Shed work runs through `cowshed exec <ws> -- <argv…>`: argv only, cwd = shed root, the child's exit passes through.
  Its default class is `background` (`--class background`): it holds a host CPU token and runs at nice +10, the effect
  `taskpolicy -b` was used for. `taskpolicy -b` itself fails with EPERM inside the sandbox; do not reach for it.
- Never `direnv exec` a shed from the host (`cd "$(cowshed path <ws>)" && direnv exec . …`). devenv evaluates a shell
  per path, `TMPDIR` and runtime directory (specs/cowshed/03_caches.md "Sandboxed shells evaluate devenv in place"), so
  a host entry is a second cold evaluation of the same shed beside the sandbox's own (measured 2026-10-20: 197 s on top
  of the shed's, concurrently), and the Nx it runs starts its daemon outside the sandbox.
- The one thing this changes for you: the command runs with the shed's authority, not yours. A path outside the shed
  (another checkout, `~`) needs `cowshed grant <ws> --read|--write <path>` first.
- Unit work that cannot run sandboxed (an APFS image test, a host-controller test) runs as
  `cowshed exec <ws> --host -- direnv exec . <cmd…>`: outside the sandbox, without shell activation (hence the
  `direnv exec`, with the host evaluation cost above), at the default background class and its background token. Never
  pass `--class gate` for unit work: the gate class takes the repository's gate order ahead of its lands, and only the
  repository's land path runs a gate.
- macOS loopback inside exec: only the shed's own port block (`$COWSHED_PORT_BASE`, 64 ports) is connectable; 127.0.0.1
  outside it is EPERM. Bind services at `$COWSHED_PORT_BASE + 1…`, and `grant --ports <N>` before starting them. Node
  and Bun port-0 listeners, and a child they start with `--inspect=…:0`, land in the block on their own through
  cowshed's preload in `NODE_OPTIONS`/`BUN_OPTIONS`; other runtimes still need a block port, or `grant --loopback-any`.
  Linux sheds get private loopback with all ports.

## Keep builds shareable

- Never point agents at a shared external build directory. `target/` is inside each workspace image and already warm; an
  external shared target restores contention and serializes builds on Cargo's per-target-directory lock.
- Never set `CARGO_INCREMENTAL`, not even for a land check. Setting it to `1` hard-fails the sccache wrapper at Cargo's
  version probe; setting it to `0` discards incremental compilation and makes every workspace crate a second unit. Leave
  it unset: workspace crates stay incremental while their dependencies use the shared host cache.
- Let `test` inherit `dev`: no `[profile.test]` overrides. `cargo test` then reuses the dependencies `cargo build`
  compiled, and the `target/` a new workspace clones from main is warm for both.
- Never compile `env!("CARGO_MANIFEST_DIR")` in, tests included. Cargo does not fingerprint the checkout path, so a
  workspace runs the test binaries it cloned from main and a baked path reads main's files. Read it at run time with
  `std::env::var_os("CARGO_MANIFEST_DIR")`, which cargo and nextest set for every test.
- After a lockfile or toolchain bump, re-warm main's image with `cowshed exec main -- <canonical build>` so new clones
  inherit the dependency graph.
- Cargo's linker must be a bare PATH program or immutable store path, never checkout-relative. Use the pinned nightly's
  checksum/content freshness consistently; switching modes invalidates units once.
- Nx inside a workspace sandbox keeps its normal daemon and the checkout's own `.nx`: host shells, read-write and
  read-only jobs share one task database, cache and daemon through the build volume. Read-only jobs keep source
  read-only, not a second Nx state. The supervisor keeps the daemon in the sandbox and host shells connect to it. The
  `NX_MAX_CACHE_SIZE` default and override policy are defined in
  [03_caches.md §Wiring](../../../../specs/cowshed/03_caches.md#wiring). Repository shells leave `NX_DAEMON` to Nx.
- The compile cache is a host daemon. Start it deliberately with `cowshed sccache start --capacity <size>`; a client
  that spawns its own daemon silently gets sccache's 10 GiB default cap. If hits are absent, run
  `cowshed sccache status` first.
- A successful managed Bun install records lockfile SHA-256 values in `node_modules/.smoo-install`. Doctor reports
  missing receipts or lock drift and never installs dependencies.

## Output and failures

- stdout is the result; stderr is progress/guidance. Use `--json` for machine output; follow each `next:` command
  literally instead of scraping stderr. Exit codes (docs/cli.md): 2 usage, 3 not-found, 4 conflict, 5
  environment-missing, 6 sandbox-denied, 7 integrity; exec passes the child's status through (wrapper failures 100–106).
- Exit 6 is a negotiation, not a retry: surface the named `next:` grant to the operator/coordinator; filesystem grants
  apply from the next exec. A `sandbox-denied` on a lifecycle verb saying this process is "sandboxed away from cowshed's
  store" means your shell is sandboxed — no grant helps; rerun from an unsandboxed shell.
- Error code 150 from a `diskutil image` command: cowshed itself runs none; it is your own
  `cowshed disk-command diskutil …` or another tool. `cowshed doctor` names leaked `AppleDiskImageDevice`s (freed by
  killing the stalled `diskimagesiod` it names, or a reboot) and orphaned helpers (freed by the `sudo kill -9` it
  names).
- A warning naming IOKit's media match-notification queue, or `doctor`'s `media-notification-cap` warning: the host's
  attached media are near or past the soft 1024 ceiling. Verbs detach idle workspaces themselves first and attach
  anyway; to free media, `cowshed rm` the sheds you are done with or `cowshed detach` the ones you will return to.
- "the cowshed daemon is build X; this cowshed is build Y": run `cowshed setup` from the build you mean, then retry.
