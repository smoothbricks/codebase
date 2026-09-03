---
name: cowshed
description:
  Give each agent, fork, or subtask an instant warm copy-on-write clone of a repository instead of a cold git worktree.
  Use when spawning parallel agents that each need their own checkout, when a worktree would force a multi-minute cold
  rebuild, when isolating risky or destructive work, or when running any cowshed command (adopt, setup, new, path, exec,
  grant, land, push, rm, gc, rebase, doctor).
---

# cowshed — warm workspaces for parallel agents

A cowshed workspace is a full standalone checkout — source, `.git`, `node_modules`, `target/`, every build cache —
cloned copy-on-write from the repository's warm `main` image in about a second. A `git worktree` gives an agent a cold
tree whose build cache starts empty; a cowshed workspace starts warm.

## Use it

Reach for cowshed when several agents work in one repository, when a build cache is expensive, or when risky work needs
a blast radius that ends at `cowshed rm`.

1. Adopt once with the warm build cache included and capacity sized for growth:
   `cowshed adopt <path> --capacity <size>`.
2. If host storage is missing, run `cowshed doctor` (it mutates nothing), then follow its `next:` command, usually
   `cowshed setup`.
3. Create one workspace per agent. Work in that workspace, never in shared `main`.
4. Retire finished workspaces and run `cowshed gc` on a cadence. Storage grows with divergence, not clone count.

direnv users need nothing extra.

## Commands

| Task                    | Command                                                              | Use                                                             |
| ----------------------- | -------------------------------------------------------------------- | --------------------------------------------------------------- |
| Create from main        | `cowshed new <name>`                                                 | Clone the warm main image.                                      |
| Create from a sibling   | `cowshed new <name> --from <ws>`                                     | Start from another workspace's current image.                   |
| Create at a stable slot | `cowshed new <name> --slot <n>`                                      | Recycle a stable mount path for path-keyed compiler caches.     |
| Locate                  | `cowshed path <ws>`                                                  | Print the live mount path.                                      |
| Run a command           | `cowshed exec <ws> -- <cmd>`                                         | Execute argv inside the workspace sandbox.                      |
| Grant host paths        | `cowshed grant <ws> --read <path...> [--write <path...>]`            | Widen filesystem access from the next exec; omit flags to list. |
| List this project       | `cowshed ls`                                                         | Show its workspaces.                                            |
| List every project      | `cowshed ls --all`                                                   | Show workspaces store-wide.                                     |
| Inspect host            | `cowshed doctor`                                                     | Check host and workspace invariants without mutation.           |
| Inspect compile cache   | `cowshed sccache status`                                             | Check daemon health and cache hits before debugging misses.     |
| Attach or detach        | `cowshed attach <ws>` / `cowshed detach <ws>`                        | Mount or park one session workspace.                            |
| Attach or detach all    | `cowshed attach --all` / `cowshed detach --all`                      | Mount or park all session workspaces.                           |
| Checkpoint or restore   | `cowshed checkpoint <ws> <label>` / `cowshed restore <ws> <label>`   | Save or roll back an image.                                     |
| Rebase                  | `cowshed rebase <ws>`                                                | Rebase the workspace branch onto main.                          |
| Land with a check       | `cowshed land <ws> --target main --check '<bare command>'`           | Rebase, check, fast-forward main, and retire on success.        |
| Deliver a branch        | `cowshed push <ws> --branch <name>`                                  | Put the workspace branch in main's repository for review.       |
| Retire                  | `cowshed rm <ws>`                                                    | Remove a landed workspace.                                      |
| Reclaim                 | `cowshed gc --dry-run` / `cowshed gc`                                | Review, then reclaim orphaned storage.                          |
| Grow an image           | `cowshed resize <ws> <size>`                                         | Grow an image; resize never shrinks it.                         |
| Rename or move          | `cowshed mv <ws> <new-name>` / `cowshed mv main <new-checkout-path>` | Rename a workspace or move the adopted checkout.                |

## Merge flow

1. Work and commit in the agent's workspace.
2. Rebase before hand-off: `cowshed rebase <ws>`. Resolve conflicts in that workspace.
3. Run the same check there: `cowshed exec <ws> -- <bare command>`.
4. Land with `cowshed land <ws> --target main --check '<bare command>'`. The check is one bare command, not a shell
   pipeline.
5. A successful `land` retires by default. If it was run with `--no-retire`, or if you used `push`, run
   `cowshed rm <ws>` after main contains the workspace `HEAD`. Do not use `--abandon` unless destroying unlanded commits
   is intentional.

## Keep builds shareable

- Never point agents at a shared external build directory. `target/` is inside each workspace image and already warm; an
  external shared target restores contention and serializes builds on Cargo's per-target-directory lock.
- Never set `CARGO_INCREMENTAL`. Setting it to `1` hard-fails the sccache wrapper at Cargo's version probe; setting it
  to `0` discards incremental compilation for no gain. Leave it unset: local dev units stay incremental while Cargo's
  non-incremental units use the shared host cache.
- A profile can be shared across workspaces or carry debuginfo, not both. Shared lanes (`test`, `release`, and gate
  builds) need `debug = 0`; debuginfo lanes stay incremental and local. Define `[profile.test]` with
  `incremental = false`, or test builds cannot be shared.
- Keep `env!("CARGO_MANIFEST_DIR")` out of shared-lane non-test code. It embeds the mount path and guarantees a cache
  miss at a new workspace path.
- After a lockfile or toolchain bump, re-warm main's image with `cowshed exec main -- <canonical build>` so new clones
  inherit the dependency graph.
- The compile cache is a host daemon. Start it deliberately with `cowshed sccache start --capacity <size>`; a client
  that spawns its own daemon silently gets sccache's 10 GiB default cap. If hits are absent, run
  `cowshed sccache status` first.

## Output and failures

- stdout is the result; stderr is progress and guidance. Use `--json` for machine output and follow each `next:` command
  literally instead of scraping stderr.
- Failures: `--json` names `code` (the taxonomy) and `hint` (the next command). Do not scrape stderr or memorize process
  exits; `cowshed --help` documents the mapping. Under `exec`, the child's status passes through.
- A missing host volume is `environment-missing`: run `cowshed doctor`, then its `next:` command. `doctor` mutates
  nothing.
