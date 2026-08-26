---
name: cowshed
description:
  Give each agent, fork, or subtask an instant warm copy-on-write clone of a repository instead of a cold git worktree.
  Use when spawning parallel agents that each need their own checkout, when a worktree would force a multi-minute cold
  rebuild, when isolating risky or destructive work, or when running any cowshed command (adopt, setup, new, path, exec,
  land, push, rm, gc, rebase, doctor).
---

# cowshed — warm workspaces for parallel agents

A cowshed workspace is a full standalone checkout — source, `.git`, `node_modules`, `target/`, every build cache —
cloned copy-on-write from the repository's warm `main` image in about a second. A `git worktree` gives an agent a cold
tree whose build cache starts empty; a cowshed workspace starts warm.

Reach for cowshed when several agents work the same repository at once, when the build cache is expensive enough that a
cold tree costs minutes, or when work needs a blast radius that ends at `cowshed rm`.

## The output contract

Every command follows one I/O discipline, and reading it correctly is most of the skill:

- **stdout** carries the one machine answer: a bare value, aligned table rows, or — with `--json` — a single envelope
  (`{"ok":true,"result":{…}}` / `{"ok":false,"error":{…}}`). Parse stdout; never scrape stderr.
- **stderr** carries progress and guidance prefixed `cowshed:` plus suggested follow-up commands prefixed `next:` —
  each hint names a command that exists in the parser. Exactly one `next:` prefix per line.
- **exit codes** are stable and are the fastest branch: `0` ok, `1` internal bug (report it), `2` usage, `3` not-found,
  `4` conflict/busy, `5` environment missing (a host volume absent or unmounted), `6` denied — a sandbox denial cowshed
  has authoritative evidence for, or a declined `setup` authorization, which means nothing changed — `7` integrity.

Under `cowshed exec`, the child's exit code passes through unchanged; failures of cowshed's own exec wrapper use
100–106 so they can never collide with a child's status.

Use `--json` whenever a result is consumed programmatically, and `-q`/`--quiet` to drop guidance while keeping hints
and errors.

`cowshed --help` lists every command and `cowshed <command> --help` prints its full grammar with one line per flag —
both on stdout, exit 0. Ask the binary before guessing a flag: the usage line is generated from the parser's own option
table, so it is never behind the CLI. A mistyped verb is corrected rather than merely refused.

## The host: one herd, two volumes

Cowshed is installed once per machine and anchored to no user account. All of its bytes live on two dedicated APFS
volumes:

- `/private/cowshed/store` — every `main` and workspace image, checkpoints, grant sidecars, and metadata.
- `/private/cowshed/caches` — the shared package and compiler caches.

At setup, one comment-tagged line per volume is appended to `/etc/fstab`
(`UUID=<uuid> /private/cowshed/<role> apfs rw,noatime,noauto,nobrowse,noowners`), so macOS mounts both at their
canonical paths before login, with no cowshed binary involved — the remounter does not live inside what it remounts.
`noowners` is deliberate: the herd is machine-global and every local account sees the same bytes, which is right for
git checkouts (git tracks mode bits, never owners) and rebuildable caches.

Workspaces other than `main` mount at `<mount-root>/<owner>/<repo>/<workspace>`; the mount root defaults to
`~/.cowshed/mnt` and is set with `cowshed setup --mount-root <dir>`.

Two LaunchAgents, `dev.cowshed.gateway` and `dev.cowshed.sccache`, run binaries copied to a host-stable install path —
never a path inside a workspace mount, which would vanish with the mount and strand launchd in a restart loop. The
gateway agent runs at load: after a reboot it validates storage, mounts every adopted project's workspaces back at
their canonical paths — each project's `main` included, because a main workspace is **always mounted** — starts the
sccache daemon, and only then serves.

A workspace's environment lives inside its image as `.cowshed/env`: plain `source`-able exports (`GOENV`,
`COWSHED_WORKSPACE_TOKEN`, and on macOS `COWSHED_PORT_BASE`), rewritten whenever cowshed rotates the token, so never
hand-edit or cache its values. direnv users need nothing per shell: cowshed writes a two-line `.envrc` sourcing that
file when the checkout has none, and appends the same single line under a marker comment when the project tracks its
own `.envrc`. No CLI verb prints these values on demand.

The service verbs are for repair and inspection, not daily use: `cowshed gateway start|stop|status|run` (an ordinary
`stop` keeps the installed binary so the next `start` is cheap; `stop --purge` deletes it) and
`cowshed sccache start [--capacity <size>]|stop|status`. The gateway starts the sccache agent itself, so a healthy host
already has both.

**When anything is off, the sequence is `cowshed doctor`, then `cowshed setup`.** `doctor` is the universal
diagnostician: version and install source, per-volume state, service health (flagging CLI-versus-daemon version skew),
workspace inventory, project checks whenever an adopted checkout resolves, and a critical finding for any `main` it
cannot reach. It mutates nothing and runs from any directory. `setup` is idempotent host repair, likewise runnable from
anywhere with no repository: it creates absent volumes, remounts detached or mis-mounted ones at their canonical paths,
validates markers, mounts every adopted project's `main` — a healthy report means every main is reachable — and pins
fstab. Before any
authorization dialog appears it announces the exact intent of every action — name, UUID, size, destination — and
everything that can require elevation happens inside that single session. On a healthy host it changes nothing and says
so; a declined dialog changes nothing and exits 6. Existing volumes are never deleted.

## Onboarding: adopt the repository

`adopt` converts an existing checkout into that repository's image-backed `main` workspace. It **renames the existing
checkout to `<root>.pre-cowshed`**, copies the whole tree into the image, and mounts the image back at the original
path. `cowshed rm main --restore` puts the original tree back and unbinds the project. Adoption scans for secrets
before copying: findings refuse the adopt (exit 4) unless `--quarantine` relocates them instead — there is no "adopt
anyway", because main's image is cloned to every future workspace.

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
what keeps the storage bounded. If the image turns out too small, `cowshed resize <ws|main> <size>` grows it in place;
it never shrinks.

Only `cowshed setup` — and, on a host where it has not run yet, the first `cowshed adopt` — may create host storage;
only those two ever raise an authorization prompt. Every other command opens storage existing-only: with a volume
absent, detached, or mis-mounted it fails (exit 5), names each volume's observed state, and prints
`next: cowshed setup`. Storage guidance across the CLI points at `cowshed setup`, never at adopting a directory — a
host with no volumes has no checkout to adopt.

## Per agent: create, locate, destroy

```sh
cowshed new agent-a                     # clone main; branch cowshed/agent-a; prints the mount path
cowshed path agent-a                    # the mount path, bare on stdout
cowshed exec agent-a -- cargo test      # run argv inside the workspace sandbox
cowshed land agent-a                    # rebase, validate, fast-forward main, retire
cowshed rm agent-a                      # retire; refuses until main contains its HEAD
cowshed gc --dry-run                    # review reclaimable storage, then `cowshed gc`
```

`new` clones main's live image with `clonefile(2)`: the clone itself is milliseconds and the attach dominates, so budget
a second or two per agent rather than a rebuild. Useful flags: `--ref <rev>` starts the branch elsewhere than main's
tip, `--from <ws>` clones a sibling workspace instead of main, `--slot <n>` mounts at a stable recycled path so
path-keyed compiler caches survive successive tenants, `--browse` shows the volume in Finder.

`ls` lists the project selected by cwd or `--project`; `ls --all` is store-wide — every adopted project on the host,
with its `owner/repo` id as the first column. `fork <src> <dst>` clones one running workspace from another.
`detach <ws>` parks a workspace at the cost of one closed file — its mountpoint directory disappears with it.
`attach` brings detached session workspaces back: `attach <ws>` mounts one, bare `attach` inside a project attaches
that project's detached sessions, and `attach --all` sweeps every project store-wide. Mains are always mounted and
never attach targets.
`checkpoint <ws> [label]` snapshots the image crash-consistently and `restore <ws> <label>` rolls back to a snapshot
(keeping the displaced image as a `pre-restore-<timestamp>` checkpoint). `mv` renames a workspace, or moves the adopted
checkout with `cowshed mv main <new-path>`. `resize` grows an image.

Verbs that act on a workspace **in place** — `rebase`, `push`, `checkpoint`, `path` — accept an omitted name and act on
the workspace your cwd stands in. Verbs that **retire, replace, move, or remount** one — `rm`, `land`, `restore`,
`mv`, `detach`, `exec` — require the name, so losing the workspace you are standing in is always something you asked
for by name rather than something the working directory decided.

Give every agent its own workspace and let `land` be the cleanup. Do not share one workspace between concurrent agents —
the branch and the working tree are single-writer.

`rm` destroys the image the workspace's commits live in, so it refuses (exit 4) unless `main` already contains the
workspace's `HEAD`. **Never answer that refusal with a flag.** `--force` overrides transient state only — a dirty tree,
an in-progress merge, a busy mount — and deliberately cannot get past the landed-ancestry gate. `--abandon` is the sole
authorization for destroying unlanded commits; it bundles them into `sessions/.trash/<ws>-<tip>.bundle` first and prints
what it destroyed.

## Which project, and where main really is

`cowshed --project /path/to/repo <cmd>` names the repository explicitly. Without it, cowshed infers the project from
your current directory — its standalone git root, validated against cowshed's repository binding — which fails when you
are outside any adopted checkout, so coordinators driving several repositories should always pass `--project`.

A project's `main` workspace **is** the primary checkout — the directory you had before adopting, always mounted at
that original path. Editing it edits the shared tree everyone lands onto; landing a workspace fast-forwards that same
directory. Treat `main` as shared state, never as your scratch space.

## Git semantics

A workspace is **not** a worktree. Its `.git` is a full independent repository, not a gitdir file pointing at a shared
object store. Inherited remotes are stripped and replaced by exactly one remote, `main`, pointing at the absolute path
of main's canonical mount. Nothing touches the network.

The branch is `cowshed/<name>`, created off the source HEAD.

This maps directly onto a rebase-then-fast-forward coordinator flow:

| Step                             | Command                                                                                                                                                         |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Agent commits                    | ordinary `git commit` inside the workspace, on `cowshed/<name>`                                                                                                 |
| Agent rebases onto main          | `cowshed rebase <ws>` — runs `git rebase main/main` inside the workspace                                                                                        |
| Coordinator lands                | `cowshed land <ws> --target main --check '<verify cmd>'` — rebases, runs the check, then `git merge --ff-only` in the host checkout, then retires the workspace |
| Deliver a branch without merging | `cowshed push <ws> --branch <name>` — delivers the branch into main's repository under a specific name                                                           |

Write the check as a **bare command** — `just verify`, `cargo test --workspace`. The sandbox `PATH` is already the
project's pinned toolchain resolved to store paths, so wrapping it in `devenv shell -q --` or a direnv re-evaluation
asks for the caller's `HOME` and a fresh evaluation, both of which the sandbox withholds on purpose; the wrapper fails
where the command inside it passes. `cowshed exec <ws> -- <cmd>` and `land --check <cmd>` are the same sandboxed exec,
so run the check in the workspace and fix what it finds before hand-off — the coordinator's verdict will match.

`land` is the whole coordinator step in one command, and it fast-forwards only: if the check fails or the merge would
not fast-forward, it stops with a non-zero exit rather than creating a merge commit. Use `push --branch` when the host
should receive the branch under a specific name for review instead of an immediate merge.

The division of labor is fixed: the WORKER runs `cowshed rebase` and resolves conflicts itself — it holds the
implementation context — so `land`'s own rebase is a no-op on a well-handed-off branch. If `land` hits a rebase
conflict, the coordinator does not resolve it: send the workspace back to its worker to rebase again. A coordinator
rebasing on the worker's behalf is what produces the diverged-branch refusal in the first place.

One failure is not a code problem: the sandbox admits only the workspace mount plus its explicit grants, so a check that
reads a path outside the workspace — an out-of-tree dependency, another repository, `~/.cargo/config.toml` — dies with
`…: Operation not permitted` even though the same command succeeds unsandboxed. When `cowshed exec` or `land --check`
fails that way, cowshed reports exit 6 and its stderr names the blocked path or domain and what would allow it: the fix
is the grant, not the code. If a project cannot build inside its grants, say so in the worker brief instead of letting
each agent rediscover it.

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

`/private/cowshed/caches` — the second volume — holds the shared package and compiler caches: cargo's registry and
git-extraction caches, sccache, Go module and build caches, and others, plus the gateway's download mirrors, which
workspaces read but never write. Shared caches are **shared across workspaces, never cloned** — correct for immutable
downloads, and the reason a warm workspace does not re-download dependencies. Build _output_ stays per workspace: it
lives in the workspace image, warm because the image was cloned from main. (bun's install cache is deliberately
in-image too, because `bun install` materializes `node_modules` by reflinking from it, and APFS clones cannot cross
volume boundaries.)

## Installing this skill

`cowshed skill install` writes this file into the skill directory of every agent harness detected on the host, and
`cowshed skill install --project <path>` installs it into a single repository instead; `--harness <name>` names one
explicitly and may repeat. It is idempotent: re-running reports `unchanged` and rewrites nothing.
