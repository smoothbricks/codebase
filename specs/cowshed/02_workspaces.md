# Workspace Lifecycle

The central convention: **main is the base**. There are no templates, no refresh pipelines, and no registration steps.
The adopted main workspace is always warm because the user works in it and merges land in it; every new workspace is a
copy-on-write clone of main's live image.

## The safe-to-clone-main invariant

Cloning main's live image for any consumer — including sandboxed agents — is safe because secrets never live in a
worktree:

- No `.env` files. Credentials exist only inside cowshed-gateway (Keychain-backed, 05_gateway.md).
- Environment wiring (`cowshed ensure`) exports build-configuration variables only, never secrets.

Everything else in main — uncommitted source changes, installed dependencies, warm build state — is exactly what makes a
clone useful.

### Secrets enforcement

The invariant is checked, not assumed — cowshed's dropped protections (jcode's direnv secret-var filtering, its
sensitive-path command scanning) are only sound if the invariant actually holds. One built-in scanner
(`cowshed-core::secrets`), three call sites:

- **`cowshed adopt` — blocking gate.** Full-tree scan before the image is created (skipping cache roots and `.git`
  objects). Findings refuse the adopt (exit 4) with per-file stderr guidance: migrate the value to the gateway Keychain,
  delete the file, or `cowshed adopt --quarantine`, which moves findings to `~/.cowshed/store/<project_id>/quarantine/`
  (0600, original paths preserved) so dependent tooling fails loudly instead of leaking silently. There is no "adopt
  anyway" flag — main's image is cloned to every future workspace; adopt is the one moment the invariant is cheap to
  establish.
- **`cowshed push` and autosave — delta gate.** Scans only content new relative to the base commit (cheap). Push refuses
  with exit 4 and names the offending hunks; autosave skips the snapshot and emits one `cowshed:` warning — it never
  blocks work, but it never propagates a finding to the host repo either. This closes the write side: an agent that
  pastes a token into a file cannot export it through cowshed.
- **`cowshed doctor` — recurring audit.** Re-scans every workspace incrementally (mtime-bounded) and reports findings
  with `fix:` hints; `--json` for fleet automation.

Detection is convention-tiered, no configuration: filename rules (`.env*`, `*.pem`, `id_*`, `.netrc`, `.npmrc`/`.pypirc`
with auth keys, cloud credential dirs) plus content rules for well-known token shapes (`ghp_…`, `xoxb-…`, `sk-…`,
`AKIA…`, PEM blocks) and `.envrc`/shell exports of `*_TOKEN|*_SECRET|*_KEY` variables. No entropy heuristics — cowshed
prefers zero false positives over exhaustiveness, and says so; `cowshed doctor --secrets-deep` shells out to `gitleaks`
when installed for the paranoid pass. Waivers are explicit and controller-owned: per-path entries in the project's
waivers file next to the grant files (never inside the image), each with a required `reason`, shown by `cowshed doctor`
so waived paths stay visible.

## `cowshed adopt` — create the main workspace

Converts an existing checkout into an image-backed workspace mounted at its original path. One-time, copy-bound
(clonefile cannot cross volumes). The source is a _live_ tree — editors, watchers, and build daemons mutate it during
the copy — so adopt is an explicit transaction with defined crash points, not a best-effort script:

1. Verify: git root, clean-enough state (adopt refuses mid-merge/rebase), free space, the secrets gate (see "Secrets
   enforcement" — blocking; `--quarantine` to relocate findings), and that `<root>.pre-cowshed` does **not** already
   exist (exit 4 — a previous adopt left state behind; resolve it first). Ensure both dedicated volumes exist — lazily
   create and mount `cowshed.store` and `cowshed.caches` (idempotent; 01_storage.md) before any image is created.
2. Create the image under a staged, non-enumerated name (`<project_id>/.staging/main.asif` — the readdir registry never
   sees staged objects), attach at a staging mountpoint.
3. Copy the full tree (including `.git`), preserving metadata, in delta passes until quiescent: re-run rsync-style delta
   copies while the source keeps changing; refuse (exit 4) if the tree fails to quiesce within the pass budget, naming
   the churning paths.
4. Write `.cowshed/workspace.json` (`role: "main"`), mint `.cowshed/token`, create in-image cache roots, write the
   shared-cache env wiring into tool config files (03_caches.md). Verify the copied tree against the source before
   publication.
5. Publish: move the original tree aside (`<root>.pre-cowshed`), recreate the emptied directory as the mountpoint with
   the self-healing stub `.envrc` inside it, rename the staged image into place, `fsync` the parent directories around
   each rename, attach.
6. Print the mount path on stdout.

Every step is idempotent to re-run. `cowshed doctor`/`cowshed gc` recognize each crash point — staged image present,
tree moved but unattached, marker unpublished — and resume or roll back. `<root>.pre-cowshed` is retained until the user
deletes it; cowshed never auto-deletes it.

Adopting is reversible: `cowshed rm main --restore` detaches and moves `<root>.pre-cowshed` back.

## `cowshed new <name>` — create a session workspace

Budget: ≤ 1 s cold. No pool, no pre-warming.

1. `flock` main's image lock; `fsync`/`sync` the main volume. Measured: the sync is for **freshness, not consistency** —
   a live clone is always crash-consistent, but without a sync it can miss the last writes (a just-written file was
   absent from a non-synced clone).
2. `clonefile(main.asif, sessions/<name>.asif)` — ~2 ms regardless of content size.
3. Attach without mounting, run `fsck_apfs -q` against the clone's APFS volume device, then mount at
   `~/.cowshed/mnt/<project_id>/<name>` — the attach tool dispatches on `imageFormat` (`diskutil image attach --noMount`
   for ASIF, `hdiutil attach -nomount` for SPARSE; flags per 01_storage.md) — ~235–400 ms typical. Verification precedes
   the first mount; a clone never mounts unchecked.
4. On fsck failure, delete the clone and retry once from a fresh sync. (Measured: 10/10 clonefiles taken under a
   continuous writer plus a streaming 128 MiB dd passed both `fsck_apfs -q` and a full `-n` check, mountable and
   readable, on both formats — this path is a safety net that is expected to essentially never fire; the fork-mid-write
   integration test pins it, 08_testing.md.)
5. Rewrite `.cowshed/workspace.json` (`role: "workspace"`, `baseCommit` = main's HEAD), mint a fresh `.cowshed/token`,
   allocate a contiguous 16-port block from the reserved range (default `40960–49151`) and record it as `portBlock` in
   the grant file (04_sandbox.md/05_gateway.md) — the gateway binds the block base as this workspace's data-plane
   listener; the remaining ports are the workspace's bindable service ports (dev servers, `devenv up`) — create the
   sibling grant file with the closed baseline (04_sandbox.md), and mark `<mount>/.envrc` direnv-trusted (trust is keyed
   by absolute path; every clone needs its own allow entry — 04_sandbox.md, devenv section). In-image config (bunfig
   registry, cargo source replacement) is written pointing at the block base; there is no git network wiring to write —
   workspace git is local-only (see "Remote code ingress").
6. Inside the mount, under the workspace's closed sandbox: `git remote add host <projectRoot>` (idempotent),
   `git switch -c cowshed/<name>` from the checked-out state. The `.git` directory arrived complete via CoW — the
   workspace is a standalone repository with **no linked-worktree registration and no back-references** into the host
   checkout.
7. Print the mount path on stdout; guidance and `next:` hints on stderr.

Flags: `--ref <rev>` (after branching, `git switch -c cowshed/<name> <rev>` instead of main's state),
`--from <workspace>` (clone a session instead of main — sugar over `cowshed fork`), `--browse`.

## `cowshed fork <src> <dst>`

Clones a _session_ mid-flight: same steps as `cowshed new` but the source image is `sessions/<src>.asif` and the marker
records `forkedFrom`. Grants do **not** carry over — the fork starts from the closed baseline, including a freshly
allocated port block (the fork never inherits `<src>`'s `portBlock`). Two divergent futures from one warm state, ~250
ms.

## `cowshed checkpoint <ws> [label]` / `cowshed restore <ws> <label>`

- Checkpoint: `sync` the workspace volume (freshness — a non-synced clone can miss the last writes; consistency is
  unconditional, see `cowshed new` step 1), `clonefile` its image to `checkpoints/<ws>/<label>.asif` (label defaults to
  a UTC timestamp). The workspace keeps running; the checkpoint is crash-consistent and fsck-verified in the background.
- Restore: detach the workspace, rename its image aside, `clonefile` the checkpoint back into place, re-attach, rewrite
  marker + token, and re-assert the workspace's port block (identity is preserved across a restore, so the `portBlock`
  and grant binding carry through). The displaced image becomes `checkpoints/<ws>/pre-restore-<ts>.asif` so a restore is
  itself undoable.
- `cowshed gc` prunes checkpoints beyond the newest 5 per workspace (`.cowshed.toml` `checkpoints.keep`).

## `cowshed push <ws> [--branch <name>]`

Delivers the workspace branch to the host repository — **pull-based**: the controller runs
`git -C <main mount> fetch <ws mount> +cowshed/<name>:cowshed/<name>` (default: the workspace's `cowshed/<name>`
branch). Refuses to update any branch currently checked out in main (exit 4). After a successful push the work exists in
the host repository's object store — the workspace is disposable from that moment.

The direction is a security invariant, not an implementation detail: **cowshed never executes git with its cwd or config
inside a workspace unsandboxed.** A workspace's `.git/config` and hooks are agent-writable, and git executes them
(`core.fsmonitor`, `core.sshCommand`, `credential.helper`, `pre-push`) — controller-side git run inside a workspace repo
would be arbitrary code execution outside the sandbox. Fetching _from_ a workspace parses objects without executing the
remote's config; that is the safe direction, and every trust-boundary git operation uses it. Workspace-side git — the
rebase step, branch creation on a fresh clone of trusted main — runs **inside** the sandbox.

## Autosave

A per-project launchd user agent (installed at adopt; systemd user timer on Linux) snapshots every _attached_ workspace
into the host repository on a 10-minute cadence (`.cowshed.toml` `[autosave] interval`). The mechanism is a host-side
fetch — the workspace branch plus `+HEAD:refs/cowshed/<ws>/wip` — the same pull-based direction as `cowshed push`, for
the same trust reason. `refs/cowshed/…` (singular) is the canonical namespace; `land` prunes a workspace's refs at
retire. Each snapshot is gated by the secrets delta-scan (see "Secrets enforcement"): a finding skips that workspace's
snapshot with one `cowshed:` warning and never blocks work. Uncommitted work between autosaves is the only loss window —
images are excluded from backup by design (01_storage.md).

## Remote code ingress: `cowshed repo`

Sandboxed git speaks only local paths. A workspace's remotes are the `host` remote (main's mount) and read-only bare
mirrors on the caches volume — never a network URL. There is no git endpoint on the gateway data plane, no credential
helper, and no `insteadOf` rewriting inside any workspace.

- **`cowshed repo mirror <url>`** — a control-plane RPC, not workspace git. The gateway checks the workspace's repo
  grants (`cowshed grant <ws> --repo github.com/org/*` — repo-scoped, finer than host egress), executes the fetch itself
  with Keychain-held credentials into a bare mirror it owns at `~/.cowshed/caches/git/<host>/<org>/<repo>.git`, writes
  one audit line, and returns the mirror path on stdout. Mirrors are created by the gateway with its own config — no
  agent-writable git config is ever in the loop — and are fetch-only, sandbox-read-only (01_storage.md, 05_gateway.md).
- **`cowshed repo clone <url> [dir]`** — sugar: mirror, then a local `git clone --dissociate <mirror>` run inside the
  sandbox.

The project's own origin is just another mirror — mirroring it covers "the agent needs `refs/pull/123/head`". GitHub API
operations (creating PRs, issues) are coordinator work in v1: they require credentials and therefore live outside the
sandbox by construction.

## Return to main: `cowshed rebase` and `cowshed land`

Workspaces are born from main and return to main. `new`/`fork` are the first half of the lifecycle; `rebase` and `land`
are the second. Between them, autosave refs (fetched host-side to `refs/cowshed/<ws>/wip` — see "Autosave") remain the
crash-safety net. cowshed provides the _primitives_; the retry/scheduling policy around them is coordinator work
(12_mcp.md), deliberately not baked in.

### `cowshed rebase <ws>`

Brings a workspace's branch up to current main.

- Default: `git fetch host` inside the workspace, then `git rebase host/main` onto the `cowshed/<name>` branch. The
  whole step runs **inside the workspace sandbox** — the fetch from main's mount is a read (covered by the baseline),
  and every write lands in the workspace's own repo. Conflicts abort the rebase (`git rebase --abort`), leave the
  workspace exactly as it was, and exit 4 with `next:` hints naming the conflicted paths.
- `--fresh`: divergence-shedding rebase. Create a new clone from **current** main, replay the branch onto it
  (`git format-patch`/`am`, or cherry-pick range), then **transplant identity** — the new clone inherits the workspace's
  name, canonical mount path, token, and grant-file binding; the old clone is destroyed (11_shell.md teardown ordering).
  This drops the accumulated substrate divergence (ZFS origin-snapshot pin, APFS image growth — 01_storage.md,
  09_substrates.md) and re-warms in-image caches to current main. A replay conflict leaves the **original** workspace
  untouched and discards the half-built fresh clone (exit 4). Refuses (exit 4) when the workspace has uncommitted
  changes — replay carries commits only; commit or discard first. `--fresh` is what a coordinator runs against a
  long-lived workspace that `cowshed du` shows has drifted far from main.

### `cowshed land <ws> [--check <cmd>]`

The full born-from-main-return-to-main close-out, as one primitive:

1. **Rebase** onto `host/main` (the `cowshed rebase` step above; conflict → exit 4, workspace intact).
2. **Validate**: run the check _inside the sandbox_ — `--check <cmd>` if given, else `.cowshed.toml` `[land] check`,
   else no validation with one honest `cowshed:` stderr line saying so. Non-zero check → exit 4, workspace intact,
   output captured as a job (11_shell.md) for diagnosis.
3. **Fast-forward host**: the host repository fetches the branch from the workspace mount and
   `git merge --ff-only cowshed/<name>`. Requires a clean-enough host worktree (refuse exit 4 otherwise, `next:` hint to
   commit/stash). If main advanced during validation the merge is non-fast-forward → exit 4 with a
   `next: cowshed land <ws>` hint; the caller (coordinator) re-runs, which re-rebases against the new main. cowshed does
   not loop internally.
4. **Retire**: destroy the workspace (supervisor tree first — 11_shell.md), prune its `refs/cowshed/<ws>/*` on the host.

`--no-retire` keeps the workspace after a successful land (for a coordinator that wants to reuse it via
`rebase --fresh`). `--push-only` performs steps 1–2 and pushes the validated branch without the host ff-merge, for
review-gated flows.

## `cowshed rm <ws>` — perceived-instant deletion

1. Refuse (exit 4) when the branch has commits absent from the host repository, unless `--force` — the unsaved-work
   safety net.
2. Stop the supervisor: TERM → grace → KILL across the whole descendant tree (11_shell.md). Teardown precedes retirement
   — live children would otherwise hold the mount busy and keep enforcing stale launch-time authority after the grants
   disappear.
3. Logically retire: rename the image to `sessions/.trash/<ws>.<ts>.asif` and remove the grant file — the workspace
   disappears from `cowshed ls`; the command returns here (typically well under a second; a stubborn process tree delays
   it by at most the kill grace).
4. Background (spawned detached): detach the mount (escalating to `-force` after a 10 s grace), unlink the trashed image
   and any checkpoints, remove the mountpoint dir. Interrupted cleanup is resumed by `cowshed gc` (idempotent).

`cowshed rm main --restore` is the adoption rollback described above; plain `cowshed rm main` requires `--force` and a
clean `git status` (exit 4 otherwise).

## `cowshed attach` / `cowshed detach`

Explicit mount lifecycle for long-lived workspaces: `detach` frees the attachment (image and grants persist); `attach`
re-mounts at the canonical path with canonical flags. Personal workspaces are typically attached at login by a launchd
agent installed by `cowshed adopt`.

## `cowshed ensure` — the .envrc fast path

Contract: **silent exit 0 in ≤ 25 ms when healthy**. Fast path is one `statfs` of the cwd plus one read of
`.cowshed/workspace.json`; no git, no network, no subprocess.

When something is wrong, `ensure` fixes what is safe to fix synchronously:

- image exists but not mounted (reboot, eject) → re-attach and continue;
- mounted with wrong flags or at the wrong path → re-mount;
- `cowshed.store` or `cowshed.caches` volume missing or mounted with wrong flags → recreate (lazy, idempotent) and/or
  re-mount with canonical flags (01_storage.md);
- broken in-image cache config → rewrite (03_caches.md).

It warns (one stderr line each, never blocking) when the gateway is unreachable or the workspace has old unpushed work.
It never does slow or surprising work — no fetch, no compaction, no refresh.

`cowshed ensure --envrc` additionally prints `export` lines on stdout — but wiring is carried by in-image config files
and host-level paths, not environment (03_caches.md pins the exact set: at most `COWSHED_GATEWAY_TOKEN`, only until its
verification passes — the bun cache export is already retired, bunfig's relative cache dir is verified — plus the
`PORT`/`COWSHED_PORT_BASE` dev-server conventions and optional non-load-bearing identity conveniences like
`COWSHED_WORKSPACE`). Anything that needs identity derives it from cwd via `.cowshed/workspace.json` or asks the CLI.
Usage in a repo's `.envrc`:

```bash
eval "$(cowshed ensure --envrc)"
```

**Self-healing stub**: the underlying (shadowed-when-mounted) mountpoint directory contains a one-line `.envrc` —
`cowshed ensure --attach` — so `cd` into an unmounted workspace triggers direnv, cowshed re-attaches, and the real
`.envrc` shadows the stub on the next prompt. Every `cd` is a repair opportunity.

## Tradeoffs

**Templates rejected.** A template registry (SHA-keyed images, refresh pipelines, promotion, rollback) reproduces CI
inside a workspace manager and demands configuration. Main-as-base gives a fresher starting state for free: main is warm
because it is used. The secrets invariant is what makes this safe; it is enforced, not assumed.

**Linked git worktrees rejected.** They couple every workspace to the host `.git/worktrees/<id>` (sandbox holes, stale
registrations, invalid when cloned). A standalone `.git` arrives via CoW at zero marginal cost, and the pull-based
`cowshed push` replaces shared-state semantics with an explicit, auditable hand-back.

**Gatewaying git rejected — workspace git is local-only.** A smart-HTTP credential broker on the gateway would let
sandboxed git reach real remotes, reintroducing in-sandbox push, a git wire protocol to proxy faithfully, and
host-granularity policy that git defeats: Anthropic's sandbox-runtime README warns that allowlisting github.com lets a
process push to any repository (https://github.com/anthropic-experimental/sandbox-runtime), and yolo-cage had to build
an entire git-command classifier to police in-sandbox remote git (https://github.com/borenstein/yolo-cage). Local-only
git removes the class: ingress is a gateway-executed mirror fetch (`cowshed repo`), egress is a controller
fetch-from-workspace (`cowshed push`), and no credential or remote URL ever exists inside a sandbox.

**hdiutil shadow files rejected.** Shadows share only a frozen base (identical to what clonefile already provides),
measure 2.3× slower on synchronous writes, and pin their base image against deletion. Clones have no runtime dependency
on their source — main's image can be replaced or compacted freely.
