---
name: cowshed
description:
  Give each agent, fork, or subtask an instant warm copy-on-write clone of a repository instead of a cold git worktree.
  Use when spawning parallel agents that each need their own checkout, when a worktree would force a multi-minute cold
  rebuild, when isolating risky or destructive work, or when running any cowshed command (adopt, setup, new, fork, path,
  exec, grant, land, push, rm, gc, rebase, checkpoint, restore, attach, detach, defrag, reseed, doctor, sccache).
---

# cowshed — warm workspaces for parallel agents

A cowshed workspace is a full standalone checkout — source, `.git`, `node_modules`, `target/`, every build cache —
cloned copy-on-write from the repository's warm `main` image. A `git worktree` starts cold; a shed starts warm. On a
large, long-used repo `cowshed new` still takes tens of seconds: APFS copies main's extent map at ~12 µs/extent on first
write, then setup walks the tree for escaping symlinks. `cowshed defrag main` is the remedy when `doctor` warns
`main-extents`.

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

| Task                     | Command                                                                            | Use                                                                                                                                                          |
| ------------------------ | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Create from main         | `cowshed new <name>` (`--ref <rev>`)                                               | Clone the warm main image at main's tip (or a rev).                                                                                                          |
| Create from a sibling    | `cowshed new <name> --from <ws>`                                                   | Clone another workspace's image. Same repo only.                                                                                                             |
| Fork live state          | `cowshed fork <src> <dst>`                                                         | Divergent future from a running workspace; grants are NOT inherited.                                                                                         |
| Create at a stable slot  | `cowshed new <name> --slot <n>`                                                    | Stable mount path — only for crates/tooling that bake absolute paths; cargo ≥1.97 + bundled sccache already hit across paths.                                |
| Linked worktree (escape) | `cowshed new <name> --git-worktree`                                                | One object store with main; costs checkpoint/restore, needs main mounted. Prefer a real shed.                                                                |
| Locate                   | `cowshed path <ws>` (`--no-attach`)                                                | Print the mount path; attaches a detached shed first unless `--no-attach`.                                                                                   |
| Run a command            | `cowshed exec <ws> -- <cmd>`                                                       | One argv, never a shell string, inside the sandbox. `--session`, `--background`, `--timeout`, `--stdin*` exist.                                              |
| Grant host paths         | `cowshed grant <ws> --read <p…> [--write <p…>]`                                    | Applies from next exec; omit flags to list. Grant the symlink's target, not the link.                                                                        |
| Grant network / ports    | `cowshed grant <ws> --egress <host> [--opaque]`; `--ports <N>`                     | Egress via gateway (`--opaque` for cert-pinning clients, e.g. Go on macOS). macOS: grow the port block _before_ launching services — refused while jobs run. |
| List                     | `cowshed ls` / `ls --all` / `ls --landed`                                          | `--landed` = patch-equivalence-aware safe-rm list (`ls --landed \| xargs -n1 cowshed rm`).                                                                   |
| Inspect host / cache     | `cowshed doctor` / `cowshed sccache status`                                        | Doctor names attach-pool, extent and orphan findings (below).                                                                                                |
| Attach / detach          | `cowshed attach [<ws>]` / `cowshed detach [<ws>]` (`--all`)                        | Mount or park. Detach is NOT cleanup: image, commits, grants, ports persist. Only `rm`/`land` retire.                                                        |
| Checkpoint / restore     | `cowshed checkpoint [<ws>] [label]` / `cowshed restore <ws> <label>`               | Explicit labels pin; restore keeps the displaced image as `pre-restore-*` and refuses over unsaved work.                                                     |
| Rebase                   | `cowshed rebase [<ws>]` (`--into <lane>`, `--onto <rev>`)                          | Onto main by default; `--into` onto a lane base's branch. Dirty tree refused; conflict aborts clean.                                                         |
| Land                     | `cowshed land <ws> [--check '<cmd>' …]`                                            | Check (repeatable), fast-forward, retire by default. Does NOT rebase — rebase first. `--into <lane>`, `--no-retire`, `--push-only` exist.                    |
| Deliver a branch         | `cowshed push [<ws>] [--branch <name>]`                                            | Preserves `refs/cowshed/<ws>/heads/<branch>` in main's repo for review; touches no checkout.                                                                 |
| Retire / reclaim         | `cowshed rm <ws>` / `cowshed gc [--dry-run]`                                       | `rm` refuses until main contains HEAD (ancestry or patch equivalence).                                                                                       |
| Grow / repair images     | `cowshed resize <ws\|main> <size>` (`--build <size>`), `defrag`, `reseed`, `rekey` | Resize never shrinks; `--build` grows the build volume. Reseed refreshes a stale fork seed.                                                                  |
| Rename / move            | `cowshed mv <ws> <new>` / `cowshed mv main <path>`                                 | Moving/re-identifying main needs a quiesced mount, run from outside.                                                                                         |

Also present: `mount main`, `build-state` (Nx lint), `gateway`, `credential`, `identity add`, `skill install` — run
`cowshed <verb> --help`; every verb is self-describing.

## Merge flow

1. Work and commit in the shed. Never reuse sheds across tasks.
2. `cowshed rebase <ws>` onto current main; resolve conflicts in the shed. Land will not do this for you.
3. Gate, then land — **but check for a repo land path first.** Repositories with their own land command require that
   route to rebase, gate on the _host_ (sandbox hashes differ and host-boundary tests cannot run sandboxed), and only
   then fast-forward. A bare `cowshed land` is refused when the repository requires its own route. Elsewhere:
   `cowshed land <ws> --check '<cmd>'` (repeatable; runs inside the sandbox after grant sync).
4. A successful land retires by default. If retire was refused after the fast-forward, main already moved: run
   `cowshed rm <ws>`, never land again. With `--no-retire` or after `push`, `rm` once main contains HEAD.
5. `--force` on `rm` waives only transient state (dirty tree, busy mount) and destroys the uncommitted tree; it can
   never waive containment. `--abandon` is the sole way to destroy unlanded commits (they are bundled to
   `sessions/.trash/` first). Never answer an `rm` refusal by adding a flag.
6. Lane work: fork units from the lane base (`cowshed fork <lane> <unit>`), then `rebase --into <lane>` /
   `land --into <lane>`; the lane base itself lands to main as an ordinary workspace.

## Host route vs sandbox route

- `cowshed exec` is the sandboxed route (argv only, cwd = shed root, child's exit passes through).
- Heavy host tooling often belongs on the host route: `cd "$(cowshed path <ws>)" && direnv exec . nx run …`. In
  particular `taskpolicy -b` inside `cowshed exec` fails with EPERM — run it outside:
  `cd "$(cowshed path <ws>)" && taskpolicy -b direnv exec . nx run …`.
- macOS loopback inside exec: only the shed's own port block (`$COWSHED_PORT_BASE`, 64 ports) is connectable; 127.0.0.1
  outside it is EPERM. Bind services at `$COWSHED_PORT_BASE + 1…`, and `grant --ports <N>` before starting them. Linux
  sheds get private loopback with all ports.

## Keep builds shareable

- Never point agents at a shared external build directory; `target/` is inside each image and warm. Never set
  `CARGO_INCREMENTAL` (1 hard-fails the sccache wrapper; 0 makes every workspace crate a second unit) — not even for a
  land check. Leave it unset: workspace crates stay incremental, dependencies use the shared host cache.
- Two profiles only: `dev` (tests included) and `release`; let `test` inherit `dev`, no `[profile.test]` overrides.
- Never compile in `env!("CARGO_MANIFEST_DIR")`, tests included: cargo does not fingerprint the path, so a cloned binary
  baked with main's path silently reads main's files. Read `std::env::var_os("CARGO_MANIFEST_DIR")` at runtime.
- One Nx state per checkout: host shells, sandboxed jobs and land checks of one checkout share its `.nx` cache, task
  database and daemon (clones inherit it warm through the build volume). Never delete or redirect `.nx/workspace-data` /
  `.nx/cache` to "clean" a shed — that splits the state and cold-starts everything.
- Build volumes: a land's target adopts the landing shed's build volume including `.nx/cache`; `new`/`fork` reseed from
  the source's live volume first, so a fork is warm only as of its source's last writes. Re-warm main after
  lockfile/toolchain bumps via the land path, not by hand-copying caches.
- sccache is a host daemon: jobs get `SCCACHE_DIR`/`SCCACHE_SERVER_UDS`/`RUSTC_WRAPPER` injected, and `setup` writes
  sccache's config so env-less host clients use the shared store. Start it deliberately
  (`cowshed sccache start --capacity <size>`); a self-spawned daemon silently gets the 10 GiB default. Check
  `cowshed sccache status` (per-language hits) before debugging misses.

## Output and failures

- stdout is the result; stderr is progress/guidance. Use `--json` for machine output; follow each `next:` command
  literally instead of scraping stderr. Exit codes (docs/cli.md): 2 usage, 3 not-found, 4 conflict, 5
  environment-missing, 6 sandbox-denied, 7 integrity; exec passes the child's status through (wrapper failures 100–106).
- Exit 6 is a negotiation, not a retry: surface the named `next:` grant to the operator/coordinator; filesystem grants
  apply from the next exec. A `sandbox-denied` on a lifecycle verb saying this process is "sandboxed away from cowshed's
  store" means your shell is sandboxed — no grant helps; rerun from an unsandboxed shell.
- Attach failures with `AttachPoolExhausted` / error 150: run `cowshed doctor`. Leaked `AppleDiskImageDevice`s free only
  on reboot; orphaned `diskimagesiod` helpers are freed by the `sudo kill -9` doctor names, then
  `attach-budget --clear`. Detaching sheds does not free leaked devices — do not retry-spawn into a spent pool.
- "the cowshed daemon is build X; this cowshed is build Y": run `cowshed setup` from the build you mean, then retry.
