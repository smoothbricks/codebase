---
name: cowshed
description:
  Give each agent, fork, or subtask an instant warm copy-on-write clone of a repository instead of a cold git worktree.
  Use when spawning parallel agents that each need their own checkout, when a worktree would force a multi-minute cold
  rebuild, when isolating risky or destructive work, or when running any cowshed command (adopt, new, path, exec, rm,
  gc, rebase, land, push).
---

# cowshed — warm workspaces for parallel agents

A cowshed workspace is a full standalone checkout — source, `.git`, `node_modules`, `target/`, every build cache —
cloned copy-on-write from the repository's warm `main` image in about a second. A `git worktree` gives an agent a cold
tree whose build cache starts empty; a cowshed workspace starts warm.

Reach for cowshed when several agents work the same repository at once, when the build cache is expensive enough that a
cold tree costs minutes, or when work needs a blast radius that ends at `cowshed rm`.

## The output contract

Every command follows one I/O discipline, and reading it correctly is most of the skill:

- **stdout** carries the one machine answer: a bare value, TSV rows, or — with `--json` — a single envelope
  (`{"ok":true,"result":{…}}` / `{"ok":false,"error":{…}}`). Parse stdout; never scrape stderr.
- **stderr** carries guidance prefixed `cowshed:` and one suggested follow-up prefixed `next:`. Exactly one `next: `
  prefix per line — a `next:` line is a command to consider running, not output to parse.
- **exit codes** are stable and are the fastest branch: `0` ok, `1` internal bug, `2` usage, `3` not-found, `4`
  conflict, `5` environment (storage or setup missing), `6` policy denial.

Use `--json` whenever a result is consumed programmatically, and `--quiet` to drop guidance while keeping hints and
errors.

## One-time: adopt the repository

`adopt` converts a checkout into the repository's warm `main` image. It provisions the `cowshed.store` and
`cowshed.caches` APFS volumes on first use (one administrator prompt), **renames the existing checkout to
`<repo>.pre-cowshed`**, copies the whole tree into a sparse image, and mounts that image back at the original path.
`cowshed rm main --restore` reverses it.

`adopt` has no exclude rules — it copies the entire tree — and it moves the checkout out from under anything holding its
path. Both facts make the following the normal flow, not an optional tidy-up.

**Step 1 — retire every `git worktree` first.** A worktree's `.git` file holds an absolute `gitdir:` path into the main
checkout, and the matching `.git/worktrees/<name>/gitdir` holds an absolute path back. The rename breaks both
directions, and the stale admin directories are copied into the image pointing at trees that are not in it. Merge or
remove them before adopting; cowshed workspaces replace them anyway:

```sh
git worktree list                  # expect only the main checkout when you are done
git worktree remove <path>         # or merge the branch first
git worktree prune
```

**Step 2 — garbage-collect the build cache; do not delete it.** The adopt copy is a one-time capitalization of the
image, and **the warm build cache should ride into it**: from then on every workspace inherits that cache through
`clonefile` at zero marginal cost. A lean image that forces each fork to rebuild from cold throws away the entire point.
Nothing a fork would need should be cleared.

What is worth removing is _dead_ output, not cache. A long-lived build directory accumulates stale profiles and
superseded dependency versions over months; that mass costs image capacity and copy time while no fork ever reads it.
Measure first:

```sh
du -sh /path/to/repo /path/to/repo/.git /path/to/repo/target
```

Then take either route — both end with the image holding a warm, current cache:

- **Trim in place, then adopt.** Sweep the build directory down to the artifacts a current build at `HEAD` actually uses
  (a `cargo sweep`-class tool, or removing profiles you no longer build), then adopt.
- **Adopt lean, then warm inside the image.** Adopt with the build directory reduced, then run one canonical build
  _inside_ the mounted image so it holds exactly the live cache before any fork is taken.

**Step 3 — adopt, sized for the warm cache plus growth:**

```sh
cowshed adopt /path/to/repo --capacity 400g --repo-id owner/repo
```

`--capacity` covers the live cache, the source, `.git`, and headroom for divergence: workspaces share extents at clone
time but every rebuild writes new blocks, so allocation grows with the number of workspaces and how far each one's build
diverges. Retire finished workspaces and run `cowshed gc` on a regular cadence — that reclamation, not a lean image, is
what keeps the storage bounded.

If any command exits `5` with `provision APFS volumes cowshed.store, cowshed.caches`, adopt has not run on this host.
That is the fix — not a retry.

## Per agent: create, locate, destroy

```sh
cowshed --project /path/to/repo new agent-a   # clone; creates branch cowshed/agent-a
cowshed path agent-a                          # print the mount path (bare value on stdout)
cowshed exec agent-a -- cargo test            # run argv inside the workspace sandbox
cowshed rm agent-a                            # retire it; --force if it holds unpushed commits
cowshed gc --dry-run                          # review reclaimable storage, then `cowshed gc`
```

`new` clones the image with `clonefile(2)`: the clone itself is milliseconds and the attach dominates, so budget a
second or two per agent rather than a rebuild. `ls` lists workspaces; `ensure` heals the current one; `fork <src> <dst>`
clones one workspace from another instead of from `main`.

Give every agent its own workspace and let `rm` be the cleanup. Do not share one workspace between concurrent agents —
the branch and the working tree are single-writer.

## Git semantics

A workspace is **not** a worktree. Its `.git` is a full independent repository, not a gitdir file pointing at a shared
object store. Inherited remotes are stripped and replaced by exactly one remote, `host`, pointing at the absolute path
of the host checkout. Nothing touches the network.

The branch is `cowshed/<name>`, created off the source HEAD.

This maps directly onto a rebase-then-fast-forward coordinator flow:

| Step                             | Command                                                                                                                                                         |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Agent commits                    | ordinary `git commit` inside the workspace, on `cowshed/<name>`                                                                                                 |
| Agent rebases onto main          | `cowshed rebase <ws>` — runs `git rebase host/main` inside the workspace                                                                                        |
| Coordinator lands                | `cowshed land <ws> --target main --check '<verify cmd>'` — rebases, runs the check, then `git merge --ff-only` in the host checkout, then retires the workspace |
| Deliver a branch without merging | `cowshed push <ws> --branch <name>` — `git push host HEAD:refs/heads/<name>`                                                                                    |

`land` is the whole coordinator step in one command, and it fast-forwards only: if the check fails or the merge would
not fast-forward, it stops with a non-zero exit rather than creating a merge commit. Use `push --branch` when the host
should receive the branch under a specific name for review instead of an immediate merge.

## Copy-on-write facts worth relying on

- A cloned build cache **stays warm**, including at a new path. Cloning a warm Rust `target/` and building against it is
  a no-op build, not a rebuild.
- A clone shares extents and copies only metadata: the clone allocates approximately zero bytes. Directory entries and
  inodes are still copied, so a clone of a tree with millions of files takes seconds, not microseconds.
- Space is spent on **writes**, not on the clone. A full rebuild inside a workspace eventually allocates that
  workspace's full build size, which is what `gc` reclaims.
- Because `target/` lives inside the workspace image, each agent already has a private warm build directory. Do not
  point agents at a shared external build directory — that reintroduces the contention the clone removed.
- Outside a workspace, `/bin/cp -c -R <src> <dst>` performs the same clone on APFS. Spell it `/bin/cp`: a GNU `cp`
  earlier on PATH has no `-c` and fails. This is per-file and therefore **not atomic across a directory** — quiesce
  writers before cloning a live multi-file store such as a write-ahead log plus its sidecars. A workspace clone has no
  such hazard: one image file is cloned, so the whole tree is captured at a single point in time.

## Caches

`~/.cowshed/caches` holds shared package and compiler caches (cargo registry and git checkouts, sccache, and others). It
is **shared across workspaces, never cloned** — correct for immutable downloads, and the reason a warm workspace does
not re-download dependencies. Build _output_ is per workspace; only the fetch caches are shared.

## Installing this skill

`cowshed skill install` writes this file into the skill directory of every agent harness detected on the host, and
`cowshed skill install --project <path>` installs it into a single repository instead. It is idempotent: re-running
reports `unchanged` and rewrites nothing.
