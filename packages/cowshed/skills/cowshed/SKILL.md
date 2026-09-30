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
cloned copy-on-write from the repository's warm `main` image. A `git worktree` gives an agent a cold tree whose build
cache starts empty and a first build that costs minutes; a cowshed workspace starts warm. `cowshed new` on a large,
long-used repository still takes tens of seconds, and nearly all of it is two steps. The clone call returns in
milliseconds, but the clone's first write, during its attach or mount, makes APFS copy the extent map of main's image
file. That copy costs about 12 µs per extent, and main's image fragments with every write it takes while clones share
its blocks: 2.1 million extents cost 25 s on a quiet host and over a minute on a busy one. Then setup walks the whole
tree for symlinks that point outside it, about 5 s over a million entries. The number of attached images does not
matter.

## Use it

Reach for cowshed when several agents work in one repository, when a build cache is expensive, or when risky work needs
a blast radius that ends at `cowshed rm`.

1. Adopt once with the warm build cache included and capacity sized for growth:
   `cowshed adopt <path> --capacity <size>`.
2. If host storage is missing, run `cowshed doctor` (it mutates nothing), then follow its `next:` command, usually
   `cowshed setup`. `cowshed setup` also refreshes the gateway service binary from the invoking build — run it from a
   release build. A daemon running a binary from before that build is host drift the repair ends, not a state it reports
   as set up.
3. Create one workspace per agent. Work in that workspace, never in shared `main`.
4. Retire finished workspaces and run `cowshed gc` on a cadence. Storage grows with divergence, not clone count.

direnv users need nothing extra.

## Commands

| Task                    | Command                                                              | Use                                                                                        |
| ----------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Create from main        | `cowshed new <name>`                                                 | Clone the warm main image.                                                                 |
| Create from a sibling   | `cowshed new <name> --from <ws>`                                     | Start from another workspace's current image.                                              |
| Create at a stable slot | `cowshed new <name> --slot <n>`                                      | Recycle a stable mount path for path-keyed compiler caches.                                |
| Locate                  | `cowshed path <ws>`                                                  | Print the live mount path.                                                                 |
| Run a command           | `cowshed exec <ws> -- <cmd>`                                         | Execute argv inside the workspace sandbox.                                                 |
| Grant host paths        | `cowshed grant <ws> --read <path...> [--write <path...>]`            | Widen filesystem access from the next exec; omit flags to list.                            |
| Grant network reach     | `cowshed grant <ws> --egress <host>`                                 | Admit one host through the gateway; separately audited.                                    |
| Grant a pinned client   | `cowshed grant <ws> --egress <host> --opaque`                        | Tunnel without interception for a client that verifies the real certificate (Go on macOS). |
| List this project       | `cowshed ls`                                                         | Show its workspaces.                                                                       |
| List every project      | `cowshed ls --all`                                                   | Show workspaces store-wide.                                                                |
| Inspect host            | `cowshed doctor`                                                     | Check host and workspace invariants without mutation.                                      |
| Inspect compile cache   | `cowshed sccache status`                                             | Check daemon health and cache hits before debugging misses.                                |
| Attach or detach        | `cowshed attach <ws>` / `cowshed detach <ws>`                        | Mount or park one session workspace.                                                       |
| Attach or detach all    | `cowshed attach --all` / `cowshed detach --all`                      | Mount or park all session workspaces.                                                      |
| Checkpoint or restore   | `cowshed checkpoint <ws> <label>` / `cowshed restore <ws> <label>`   | Save or roll back an image.                                                                |
| Rebase                  | `cowshed rebase <ws>`                                                | Rebase the workspace branch onto main.                                                     |
| Land with a check       | `cowshed land <ws> --target main --check '<bare command>'`           | Check, fast-forward main, and retire on success.                                           |
| Deliver a branch        | `cowshed push <ws> --branch <name>`                                  | Put the workspace branch in main's repository for review.                                  |
| Retire                  | `cowshed rm <ws>`                                                    | Remove a landed workspace.                                                                 |
| Reclaim                 | `cowshed gc --dry-run` / `cowshed gc`                                | Review, then reclaim orphaned storage.                                                     |
| Grow an image           | `cowshed resize <ws> <size>`                                         | Grow an image; resize never shrinks it.                                                    |
| Make clones cheap again | `cowshed defrag main`                                                | Rewrite main contiguously when doctor warns `main-extents`.                                |
| Rename or move          | `cowshed mv <ws> <new-name>` / `cowshed mv main <new-checkout-path>` | Rename a workspace or move the adopted checkout.                                           |

## Merge flow

1. Work and commit in the agent's workspace.
2. Rebase before hand-off: `cowshed rebase <ws>`. Resolve conflicts in that workspace.
3. Run the same check there: `cowshed exec <ws> -- <bare command>`.
4. Land with `cowshed land <ws> --target main --check '<bare command>'`. The check is one bare command, not a shell
   pipeline.
5. A successful `land` retires by default. If it was run with `--no-retire`, if you used `push`, or if land's error says
   it landed but kept the workspace, run `cowshed rm <ws>` after main contains the workspace `HEAD` (never land again:
   main already moved). Do not use `--abandon` unless destroying unlanded commits is intentional.

## Keep builds shareable

- Never point agents at a shared external build directory. `target/` is inside each workspace image and already warm; an
  external shared target restores contention and serializes builds on Cargo's per-target-directory lock.
- Never set `CARGO_INCREMENTAL`, not even for a land check. Setting it to `1` hard-fails the sccache wrapper at Cargo's
  version probe; setting it to `0` discards incremental compilation and makes every workspace crate a second unit. Leave
  it unset: workspace crates stay incremental while their dependencies use the shared host cache.
- Let `test` inherit `dev`: no `[profile.test]` overrides. `cargo test` then reuses the dependencies `cargo build`
  compiled, and the `target/` a new workspace clones from main is warm for both.
- A non-incremental profile (`release`, gate builds) is shared across workspaces, so it carries `debug = 0`; a profile
  with debuginfo stays incremental.
- Never compile `env!("CARGO_MANIFEST_DIR")` in, tests included. Cargo does not fingerprint the checkout path, so a
  workspace runs the test binaries it cloned from main and a baked path reads main's files. Read it at run time with
  `std::env::var_os("CARGO_MANIFEST_DIR")`, which cargo and nextest set for every test.
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
- `sandbox-denied` from a lifecycle verb such as `new` or `rm` whose message says "the process running cowshed is
  sandboxed away from cowshed's store" means the shell running cowshed is itself sandboxed (the kernel answered EPERM on
  a store path). The store is intact and no grant helps; rerun the verb from a shell whose sandbox allows writing the
  store. Any other `sandbox-denied` names its own refusal (a grant that would intersect a protected root, a shell input
  that escapes the workspace): follow its hint.
