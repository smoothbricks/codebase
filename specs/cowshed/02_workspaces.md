# Workspace Lifecycle

The central convention: **main is the base**. There are no templates, no refresh pipelines, and no registration steps.
The **main workspace** is the adopted image-backed workspace and its standalone Git repository/object store. It is not
the checked-out Git `main` branch, and it is not shorthand for that branch's working tree or index. The main workspace
is warm because the user works in its currently checked-out working tree and merges land there; every new workspace is a
copy-on-write clone of the main workspace's live image plus a clone of main's latest build-volume seed. A land points
main at the landed workspace's build volume, so main never rebuilds what the workspace built (16_build_volumes.md).
Operations that update only a non-checked-out ref in its repository do not change its checked-out branch, index, or
working tree.

Every use of `main` in this specification is project-scoped. A host may adopt any number of repositories, each with its
own warm `main`, sessions, checkpoints, grants, gateway identity, and standalone Git object store under its primary
`repo_id`. Commands select the project explicitly with global `--project <git-root>` or derive it from cwd; `fork`,
`checkpoint`, `restore`, `push`, and `land` never select a machine-global main or cross repository boundaries.

## The safe-to-clone-main invariant

Cloning main's live image for any consumer — including sandboxed agents — is safe because secrets never live in a
worktree:

- No `.env` files. Credentials exist only inside cowshed-gateway (Keychain-backed, 05_gateway.md).
- Environment wiring (`.cowshed/env`, sourced by `.envrc`) exports build-configuration variables only, never secrets.

Everything else in main — installed dependencies, build outputs, warm build state and uncommitted work — makes a clone
useful. A fork carries its source's tracked edits and untracked files unless asked not to (`--clean`, or `--ref`); main
is no exception. Uncommitted work is warm state like any other.

With `--clean`, a standalone clone resets tracked changes from `HEAD` before other mint steps and deletes nonignored
untracked paths. An inherited git-worktree clone first establishes its own registration and index, then cleans through
that clone-owned Git before publication; it never resets through the source's inherited pointer or changes its index.
Ignored paths and build-state links stay warm. The reset rewrites only changed files, preserving other mtimes. The clone
is its volume's root, where macOS keeps bookkeeping (`.Trashes`, `.fseventsd`, `.Spotlight-V100`, `.TemporaryItems`,
`.DocumentRevisions-V100`), which a mount may grow root-owned. Cowshed excludes each in the clone's `info/exclude`
before cleaning and whenever its supervisor starts: in the standalone checkout's own Git directory, or main's common
directory for a linked worktree.

### Secrets enforcement

The invariant is checked, not assumed. One built-in scanner (`cowshed-core::secrets`), three call sites:

- **`cowshed adopt` — blocking gate.** Full-tree scan before the image is created (skipping cache roots and `.git`
  objects). Findings refuse the adopt (exit 4) with per-file stderr guidance: migrate the value to the gateway Keychain,
  delete the file, or `cowshed adopt --quarantine`, which moves findings to
  `/private/cowshed/store/<owner>/<repo>/quarantine/` (the primary component-safe `repo_id` path; mode 0600, original
  paths preserved) so dependent tooling fails loudly instead of leaking silently. There is no "adopt anyway" flag —
  main's image is cloned to every future workspace; adopt is the one moment the invariant is cheap to establish.
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

Converts an existing checkout into that project's image-backed `main` workspace, in one of the two checkout layouts
below. One-time per project, copy-bound (clonefile cannot cross volumes). The source is a _live_ tree — editors,
watchers, and build daemons mutate it during the copy — so adopt is an explicit transaction with defined crash points,
not a best-effort script:

1. Verify: git root, clean-enough state (adopt refuses mid-merge/rebase), free space, the secrets gate, and repository
   identity. Normalize every configured remote to a lowercase `owner/repo` candidate. With no remotes,
   `--repo-id owner/repo` is required and the primary binding records no fabricated remote name or URL. With remotes, an
   explicit `--repo-id` must match a candidate; without it, all candidates must normalize to one identity. `origin` may
   break a tie only among remotes for that same identity. Record the selected remote name and its credential-, query-,
   and fragment-free URL, and validate that URL still normalizes to the recorded primary `repo_id`. Programmatic callers
   supply the same explicit identity as `AdoptOptions.repo_id`; the coordinator rejects any disagreement with the
   provisional binding before image mutation. An explicit `--re-adopt` with no `--repo-id` binds the checkout's existing
   primary identity instead (see Re-adoption, step 1). Load trusted policy only from
   `/private/cowshed/store/<owner>/<repo>/policy.json`. Also require that `<root>.pre-cowshed` does **not** already
   exist (exit 4 — a previous adopt left state behind; resolve it first). Ensure host setup is present — declaratively
   validated when home-manager/nix-darwin owns it (`programs.cowshed`/`services.cowshed`, 14_nix.md), imperatively
   applied otherwise — and the dedicated store volume exists: run the same host setup `cowshed setup` runs, which
   creates and mounts `cowshed.store` at `/private/cowshed/store` (the volume marker in 01_storage.md) before any image
   is created.
2. Create the image (case-sensitive APFS in sparse RAW, 01a_apfs.md) at its canonical name, `<owner>/<repo>/main.dmg`,
   behind a fence: its complete sibling host sidecar is written first, in publication state `PendingFence`, which keeps
   main out of ordinary enumeration, gateway inventory, and every verb that would mount or serve it. Both path
   components come from the validated primary `repo_id` and are encoded independently as specified in 01_storage.md. The
   image is minted (01a_apfs.md "Images"): one `clonefile` of the store's formatted blank template takes the canonical
   name whole, so the name only ever holds a complete, formatted volume. One exact-image
   `diskutil image attach --nobrowse --noMount --plist` attaches it, pinned `fsck_apfs -q` verifies it, then
   `mount_apfs` mounts its exact owned volume at a staging mountpoint (01a_apfs.md §Mounts). That attach is the only one
   main gets.
3. Copy the full tree (including `.git`), preserving metadata, in delta passes until quiescent. A bounded worker pool
   sized to available cores processes independent leaves. Each leaf first requests an APFS metadata clone and falls back
   only that leaf to `copyfile` data copy on `EXDEV`/`ENOTSUP`, so one cross-volume subtree never aborts completed
   siblings. Re-run delta passes while the source keeps changing; refuse (exit 4) if the tree fails to quiesce within
   the pass budget, naming the churning paths. Quiescence compares source identity, length, mode, links, mtime and
   ctime, not allocated blocks: filesystem writeback or compression can change allocation without a repository write.
   Allocation remains separate fidelity and space-accounting evidence.
4. Write `.cowshed/workspace.json` (`role: "main"`), mint `.cowshed/token`, mint main's per-workspace CA (private key
   controller-side next to the grant file; CA cert placed in-image as a trust anchor with the tool anchors wired —
   04_sandbox.md/05_gateway.md), create in-image cache roots, and write platform endpoint plus shared-cache wiring into
   tool config files (03_caches.md). Set the copied repository's `core.ignorecase` to `false` — the value `git init`
   probes on the image's case-sensitive volume; a checkout that lived on a case-insensitive volume carries `true`, and
   git relies on it matching the filesystem. The main sidecar contains `portBlock` only on macOS; Linux omits it and
   creates its per-incarnation socket/netns connector when the published workspace is attached. Verify the copied tree
   against the source before publication.
5. Publish, building every durable artifact before the user's tree is touched: unmount the exact verified staging volume
   with `diskutil unmount`, then activate the sidecar from `PendingFence` to `Active` in one atomic rewrite — the
   publication point. Only then does the checkout path change hands, in the way the chosen layout prescribes, and the
   same pinned attachment mounts there with `mount_apfs`. The guards recheck exact image/device ownership after the host
   lease wait, and the held mount root/source is verified before handing the checkout back (01a_apfs.md §Mounts). The
   normal path has no detach, image rename, second attach, or second fsck: an attached image is never renamed, because
   its identity is the backing path it was attached from.

### Re-adoption: format transition and space reclamation

`cowshed adopt <checkout> --re-adopt` replaces an adopted main through the same staged copy, verification, publication
fence and checkout handoff as adopt. Default adopt remains idempotent; replacement is explicit. Re-adoption is the
ASIF-to-RAW transition and the RAW guest-space reclaim operation, not an image converter or a second runtime format.

1. Require the caller to be outside the checkout and require its source and build volumes to have no holders. Refuse
   with each holder's PID and command; never kill a user process or force an unmount. Each mounted image's holder census
   runs under its existing image lease (01a_apfs.md, "Mounts" and "Ownership, identity, and the volume label"), after an
   in-flight cosmetic rename releases its mount descriptor. Supervisor retirement remains nonwaiting; genuine holders,
   including the controller's own PID, still refuse. An unmounted input is not censused. Record the exact source backing
   path, incarnation, original checkout path, build-volume identity, current kernel IOMedia capacities and retained
   paths in the durable adoption intent before changing any of them. Preserve an existing `<checkout>.pre-cowshed` tree
   without overwriting or deleting it. Source and build copy mounts use the project's recorded adoption staging
   namespace, with source and destination names that cannot be parsed as BuildVolumeIds. Without `--repo-id`, the
   replacement binds the checkout's existing primary `repo_id` from the store rather than deriving one from the remotes.
   It does so only when the in-image marker at the checkout records this checkout as main's, the controller's Active RAW
   `main.dmg` sidecar for that repository records the same checkout and incarnation, a binding owns that repository, and
   no other project's RAW main claims the checkout. Any disagreement refuses. With no main marker at the checkout, or no
   RAW `main.dmg` for the repository it names, the identity rules of adopt step 1 apply.
2. Record the source's complete content and Git state while it is quiescent: HEAD, porcelain, count-objects, local
   config including includes, every ref, packed refs or reftables, reflogs and stash, hooks, `info/`, index and worktree
   registrations. Preserve the sheds' links to the same main checkout path. Detach the exact old source without force,
   retain its backing image and sidecars under the recorded retained name, then attach that image read-only at the
   recorded source staging mount. The original checkout becomes the publication target, not the copy source.
3. Mint a fresh sparse RAW image behind `PendingFence` and copy the entire source filesystem, including ignored build
   state and all of `.git`, directly into its staging mount. Preserve sparse leaves through metadata clones or
   hole-aware copies; do not materialize the nominal image capacity or use an intermediate full checkout on Data. The
   main's build-volume identity and owner record are preserved verbatim, including its built-at tree and creation time;
   its contents are carried into a sparse RAW backing image by the same staged, verified copy at the recorded capacity.
   Nothing substitutes an empty build volume for the recorded warm one, except for a missing main (below) and a lost
   build input. A recorded build volume or old-incarnation seed whose image is at neither its original nor its retained
   path is lost: retention moves only its record, if one is beside the original. Past retention, a build volume with no
   retained image is published as an empty RAW volume under the recorded ID, `Linked main` owner and creation time, with
   its built-at tree cleared, and any staged copy an earlier attempt made of it is discarded wherever the replay meets
   it, because nothing can verify it any more. A build replacement already published at the build image path under the
   recorded owner record verbatim no longer reads its retained image: losing that image afterwards keeps the published
   replacement and its record. A lost main image refuses as before. Leave old-incarnation seed images unchanged as
   retained evidence; publish one fresh RAW seed from the verified warm replacement for the new incarnation. A resumed
   copy leaves already-correct hard-link topology untouched. It relinks only missing, mismatched or recopied-primary
   aliases and restores every changed alias's ancestor metadata after the subtree is complete. Already-mirrored file
   payloads are not recopied merely because the transaction was interrupted; full content and metadata verification
   still runs before publication. The exclusive claim on the recorded source build image's `<image>.hold` stays held
   through retention, copy, seed capture and publication. A seed of a volume published at that image's own path, warm or
   empty, captures under that same typed claim rather than opening a second exclusive claim on the same resource. A
   legacy ASIF source's hold is distinct from the published RAW hold: the old claim remains alive while seed capture
   independently claims the new RAW resource. A published replacement or kept build already detached is seeded before
   its first mount, not mounted merely to detach and restore it for capture. The claim remains held through that mount
   and the new seed's captured-clone freshness mark. This immutable captured-clone clock replaces live-source
   re-stamping after mount. A replay keeps an existing seed's mark: it never declares old captured bytes current by
   sampling the live image again.
4. Verify the copied content before publication, including tracked and untracked-not-ignored file checksums, the entire
   `.git` state above, `git fsck`, and the build contents and identity. Re-adoption does not normalize Git
   configuration, delete lock files, prune worktree registrations or replace user hooks. Only cowshed's own marker and
   credentials are regenerated as part of the new incarnation. Check host allocation with `du` and `st_blocks * 512`,
   not apparent file length. Any mismatch refuses publication and leaves the retained source available. Preservation
   probes never execute an external clean, smudge or process driver: status runs with every configured filter driver
   disabled. The verifier compares the copy's filter-free status and its include-resolved local config with the
   source's, both read at verification time, because an include can name a file inside the checkout, which is an empty
   mountpoint until publication. The raw local config is compared with the recorded one, then the `.git` directory byte
   for byte (a linked worktree's `.git` file must reach the same administrative directory and index, which are read in
   place and never written), and every tracked and untracked-not-ignored path's raw bytes. Retain the complete raw
   count-objects probe. Its loose-object `size` is allocated disk space, so that value may differ after writeback or
   copying; every count, pack/garbage size, alternate and diagnostic must still match. Loose-object identity and
   contents remain covered by byte comparison and fsck, not by physical allocation equality.
5. Reuse adopt's atomic `PendingFence` activation and mount the verified replacement at the exact original checkout
   path. Recovery resumes the recorded replacement; it never starts an unrelated adopt or deletes a retained source.
   Verify the remounted content, Git state and build identity against the recorded before-state. Return the retained
   image path and its explicit later-removal instruction. Keep the old backing image and original `.pre-cowshed` until
   the user removes them after verification; repeated invocation resumes the transaction rather than replacing its
   evidence. Both `adopt <checkout> --re-adopt` and `mount main --repo-id <owner/repo>` reach this recovery without a
   live Git checkout, but only from the strict unfinished journal's exact repository/checkout match. After startup
   finishes that intent, retrying adopt retrieves its recorded replacement rather than beginning another reclaim or
   retained generation. A recorded retained source temporarily remounted read-only at its canonical checkout remains a
   recovery input, not an active workspace. Repairing startup recognizes it only when the strict unfinished journal's
   old repository, checkout, workspace and incarnation match its marker, and the kernel joins that exact retained image
   to a read-only mount at that checkout. Recorded main/build inputs restored read-only at their canonical paths move
   back to their recorded copy stages without force. Foreign paths and read-write retained mounts refuse.

#### Missing main image

When main's recorded image is missing (no canonical image, sidecar or CA key, and no legacy image) and a plain Git
checkout sits unmounted at main's checkout path, `adopt <checkout> --re-adopt` re-adopts main from that checkout. It
refuses unless the checkout is the root of a Git repository one of whose remotes names the project's `repo_id`, and
unless main's completed re-adoption journal for that checkout names the lost incarnation. That journal is the only
authority left for main's metadata, image capacity (unless `--capacity` is given) and recorded build-volume identity.
The repository identity and project records are kept. The checkout is the copy source of the same staged copy, step 4
verification and `PendingFence` publication as adopt, and it must have no holders, as in step 1: a process with a file,
its working directory or its executable inside the checkout refuses, named by PID and command, and nothing is stopped.
At handoff the checkout is swapped aside to `<checkout>.readopt-<replacement incarnation>`, never `.pre-cowshed`, and
kept there as the retained evidence. Main's build volume is the project's one build record owned by `Linked main`,
whatever its ID: a build volume is an image of its own, so it can outlive main's image, and every land gives main a new
one. A surviving one is kept as it is, its holders refused as in step 1, and seeded for the new incarnation. More than
one build record owned by `Linked main` refuses with integrity. Only when none survives is main's build volume published
as an empty RAW volume under the recorded ID, `Linked main` owner and creation time, with its built-at tree cleared; a
build image or record still present at that ID then refuses: this recovery never publishes over a build volume. Before
the Git state is recorded, the checkout takes main's cowshed excludes and a build link naming main's build volume, so
the copy verifies and carries the link. Old-incarnation seeds are retained and one fresh seed is frozen as in step 3.
The journal records the copy mode, so a replay resumes the same recovery. While nothing is published, the checkout is
the user's live tree: a replay before the handoff proves it unheld again and records its Git state again before it
copies, so a commit or a fetch made after an earlier attempt refused never wedges the recovery.

Re-adoption copies filesystem entries, not allocated-but-free guest blocks. Space returns only after verification and
explicit release of the old image and any snapshots sharing its blocks (01a_apfs.md, "Space reclamation"). Existing ASIF
images are retained inputs to this transaction; ordinary runtime image enumeration and attachment use RAW only.

## Checkout layouts

Main mounts at the checkout's original path — one layout, no per-project choice to record — and every other workspace
mounts at `<mount-root>/<owner>/<repo>/<workspace>`, where the mount root is the fixed per-user root of 01_storage.md
("Directory layout"): `/private/var/folders/cowshed/<uid>/mnt` on macOS, derived from the invoking numeric UID, and
`~/.cowshed/mnt` on Linux. It is derived, never configured or stored, and `setup` has no option for it; its
provisioning, the Spotlight admission rule and the refusal of a legacy custom `mountRoot` in `host.json` are specified
in [01a_apfs.md](./01a_apfs.md#stable-workspace-mount-root). The one supported access alias is an operator-created
symlink at a preferred path pointing to the canonical root; every mount stays at the canonical path. The root holds only
mountpoint directories (empty when detached) and is plain per-user directories on Data; nothing is written inside any
checkout, so no Git exclusion is required for them.

**Direct mount.** `main` mounts at the checkout's original path. Publication renames the original tree to
`<root>.pre-cowshed`, recreates an empty directory as the mountpoint without any shell hook, and attaches there. The
user's path is the real thing: `pwd -P` reports it, the gitdir physically lives under it, and Finder shows the volume
label — which is why the label is the repository's name (01a_apfs.md) rather than an encoded identity. The cost is one
mounted volume inside the user's source tree and a mountpoint that cannot be moved by `mv`.

**Why not a symlink.** Mounting main inside the store and leaving a symlink at the original path would keep the tree
movable, but Git resolves `gitdir:` conditions against the real path — the moment adopt completed, every
path-conditional rule anchored at the original checkout would stop applying, and the loss would stay undetectable
because the checkout's own resolved gitdir would already live inside the store. Identity must sit where the user's rules
look for it; direct mount is the only faithful option.

Git identity inheritance follows each `includeIf gitdir:` rule's anchor. Cowshed's probe and workspace jobs use the
canonical mount path (01_storage.md, "Directory layout"), so rules must cover it and not rely on a symlink alias of the
root; a rule anchored only at the alias is not reliable. A rule anchored at an ancestor of both the checkout and the
canonical root matches workspaces with no further configuration: on Linux `gitdir:~/` covers checkouts under HOME and
`~/.cowshed/mnt` alike, while the macOS root lies outside HOME and needs a rule that names
`/private/var/folders/cowshed/<uid>/mnt/` or an ancestor of it. Rules anchored exactly at a project root do not reach
into the mount root; cowshed surfaces that divergence instead of letting identities silently drift: `cowshed setup`
(every adopted checkout, after a repair) and `cowshed doctor` (the current project) run
`git config --list --show-origin` in the checkout and in a throwaway probe repository under the mount root, and any
config file included only in the checkout is reported by name with the pattern that would restore it. The repository's
own config travels with every clone, so a file it includes by a relative path inside the repository
(`include.path = ../tooling/workspace.gitconfig`, which Git lists as `.git/../tooling/workspace.gitconfig`) resolves in
each clone and is never reported; a relative include that climbs out of the repository lands elsewhere for a mount and
is. The answer depends only on the host's git configuration and the fixed mount root, never on a workspace, so `fork`
does not repeat it.

Linked Git worktrees share their primary checkout's config. Managed shell setup retains that primary checkout's
repo-relative `tooling/workspace.gitconfig` include; it never retargets the shared config to the linked caller's foreign
tree. A standalone gitdir pointer without a `commondir` still owns its own checkout's include. Entering a linked
worktree cannot make Main or a clone require read access to that worktree.

Cowshed does not compensate beyond reporting. Capturing the effective identity at adopt time and stamping it as
repo-local config would snapshot one moment of a configuration the user keeps editing, and would then silently diverge
from it; honoring the path condition by telling the user exactly which rule to extend is the only faithful behavior.

6. Print the mount path on stdout.

Every step is idempotent to re-run. Intent is fsynced before image mutation, and the next command that opens the project
— `cowshed gc`, `cowshed doctor --repair`, any verb — finishes the adoption; plain `cowshed doctor` reports it
(`unfinished-intent`) and changes nothing. An unpublished main resumes in place: the delta copier skips every completed
leaf, so the repository copy never starts over. Replacement is the one exception and is safe because the checkout was
never touched before activation: a fenced main whose sidecar records a different checkout root or commit, or whose
payload never became a verified APFS volume, is discarded and adoption starts fresh. A `PendingFence` main no unfinished
intent names and no process holds (intent lease and lifecycle lock both free) is retired by `gc` and `doctor --repair`
as the adopt its metadata records: image, sidecar, and CA key are reclaimed. After activation the resumable state is
_main published, checkout still the original directory, swap pending_ — recovery needs nothing from the user's tree to
finish it, and `<root>.pre-cowshed` does not exist yet. `<root>.pre-cowshed` is retained until the user deletes it;
cowshed never auto-deletes it.

| Kill window                        | Durable state                                                    | Recovery action and guard                                                                                                                                      |
| ---------------------------------- | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Before the sidecar                 | Intent only                                                      | Re-run adopt normally.                                                                                                                                         |
| After the sidecar, before payload  | `PendingFence` sidecar                                           | Startup recovery removes the sidecar-only record.                                                                                                              |
| After the clone, before its attach | `PendingFence` + formatted, unattached canonical image           | The payload is a clone of the store's blank template, empty and formatted: the ordinary verified attach mounts it and the copy starts; same incarnation.       |
| After attach, before staging mount | `PendingFence` + verified, unmounted attachment                  | The unmounted attachment is settled and the ordinary verified attach repeats before mounting; same incarnation.                                                |
| During the copy or initializer     | `PendingFence` + staging mount of the canonical image            | An exact source device and canonical flags reuse the surviving mount; the delta copier resumes; credentials and marker are idempotent; the initializer reruns. |
| After activation, before the swap  | `Active` main, attached or not, checkout still the original tree | Adopt's intent finishes it: the swap runs, then main mounts at the checkout, reusing or replacing the surviving attachment.                                    |
| After the swap, before the mount   | `Active` main, stub at the checkout, original at `.pre-cowshed`  | The swap is recognized as done; main mounts at the checkout.                                                                                                   |
| After the mount, before completion | `Active` main mounted at the checkout + pending intent           | Finishing validates the mounted marker, records the append-safe commitment, and completes the intent.                                                          |

Adopting is reversible, and reverses the same way: `cowshed rm main --restore` detaches, swaps the retained
`<root>.pre-cowshed` tree back against whatever publication left at the checkout path — the emptied mountpoint directory
— and removes the displaced artifact. The checkout path is never absent during the restore either.

The restore refuses (exit 4) when main's head is contained by neither the retained checkout's head nor a remote ref:
commits made in main after adoption would be lost with its image. `--restore --abandon` authorizes exactly that loss and
keeps it recoverable: before anything moves, main's history beyond the retained head (all of it when that head is not an
ancestor) is written to `abandoned-main-<tip>.bundle` in a `cowshed/` directory of the retained checkout's Git directory
(`<root>.pre-cowshed/.git/cowshed/` for an ordinary checkout, which the swap carries to `<root>/.git/cowshed/`),
verified, and reported as abandoned work against the retained checkout's branch. `--abandon` on main without `--restore`
is a usage error.

Unbinding is last. Removing main first records the checkout path in the project store (`checkout-root.json`), because
main's images — the only other record naming it — are retired and then reclaimed. A restore interrupted after retiring
main is finished by the same command from the checkout: the project root lookup matches that record when no main image
exists, which is what reaches a checkout with no remote. The finishing run reports no abandoned work; the bundle the
interrupted run wrote stays where it was reported. Unbinding removes terminal storage, the controller's own state and
the mount tree, then the binding, then the checkout record, so a process that dies at any point leaves either a bound
project that command reopens or none. The binding's absence is the completion: a run that finds the project already
unbound — its own open-time recovery may have finished the pending restore — succeeds without writing anything. A death
between the last two removals leaves an unbound store directory holding only `checkout-root.json`; nothing bound reads
it, and a later adoption's main image outranks it.

## `cowshed reseed` — replace only accumulated build state

`cowshed reseed <ws> [--from <target>] [--dry-run]` leaves the workspace's source and Git state in place and replaces
only its live build volume with a clone of the target's existing latest seed (`main` by default). It is not a rebase,
restore, re-adoption or seed refresh. Dirty tracked files, staged changes, untracked source, refs and HEAD stay as they
are. The mounted workspace's excluded `.cowshed/build` link is the sole source-volume change. The next build pays a cold
rebuild for work absent from that seed. A stale seed is used unchanged; an absent current-incarnation seed refuses.

Before replacing anything it refuses an admitted job or land's build hold, a held lifecycle/image lock, an unfinished
create/fork/adopt journal entry, or a filesystem holder of the old volume, naming the recorded operation or the holder's
PID and command. It never kills a job or forces an unmount. Dry-run is read-only and reports per-image allocated bytes;
those bytes are not a promise of unique APFS physical space because other clones and seeds can retain shared extents.
The exact planning, writer-exclusion, publication and accounting contract lives in 16_build_volumes.md §Workspace
reseed.

## `cowshed mv` — rename a workspace or move the checkout

Renaming is a lifecycle operation, not a filesystem accident, for the same reason `rm` is: it has to own the unmount
side. `cowshed mv <ws> <new-name>` retires the old identity and republishes under the new one — volume label, mount
path, and recorded metadata move together — and `cowshed mv main <new-checkout-path>` moves the project's checkout.
There is no separate verb per layout: one front door, symmetric with `rm`.

The second argument means what the first one decides. `main` is not a renameable workspace — its name is fixed by the
project layout — so `mv main` can only be the checkout move and its destination is a path, while every other source
names a workspace whose destination is a new workspace name. The source disambiguates before either destination is
validated, so a path is never rejected for failing the workspace-name charset and a workspace name is never resolved
against the filesystem.

The checkout path is written down in two places, and neither can be derived from the other: the in-image marker at
main's mount root and the `infoSnapshot.projectRoot` in the sidecar beside main's canonical image. Every move carries
both. Opening a project verifies that the marker's `projectRoot` matches the canonical git root and returns a conflict
(exit 4) on mismatch, before any mount: a checkout moved or copied outside `cowshed mv` is refused, never mounted as the
project its marker names.

`mv` is a rename, not a fresh fork or mint. It preserves the source branch, remotes, uncommitted work and git-worktree
registration through its clone-then-retire composition.

The existing on-disk `fork` intent has an optional `rename` discriminator. Absent or false retains historical raw-fork
recovery; true owns the complete rename transaction: raw clone, linked-registration/index handoff when applicable, and
source retirement. It is never a public fork alias or a fresh mint. The intent is durable before any mutation. The
source's supervisor admission and ownership fences remain enforced without force; dirty state alone does not refuse a
rename because the destination preserves it. In-progress Git operations still refuse.

A rename preserves the source branch, staged-only index state, working-tree edits, untracked files and ignored warm
state. Linked registration handoff is resumable before and after its durable publication, never an index reset. Neither
source nor destination may start new work while the rename is unfinished, and the destination is not served before
ownership transfer and source retirement complete. Recovery resumes the exact journaled source/destination pair rather
than creating another incarnation or discarding staged state. A rename intent still `Prepared` with no `PendingFence`
destination is discarded and named, like a refused fork; only a mutating rename or one with durable destination facts
resumes. Before source retirement, including a retry with an existing destination, the destination's HEAD and a
read-only work digest must equal the source fence. Without optional locks, the digest covers the porcelain status with
its branch header, the binary diff of the index against HEAD, the binary diff of the working tree against the index, and
the path, mode and bytes of every untracked non-ignored file. Links rewritten by inherited-link restoration compare by
path only. Linked worktrees must also have equal index-byte digests. A mismatch keeps both workspaces and the unfinished
intent, with a rename-specific preservation refusal. An untracked nested repository is Git's `??dir/` entry: its
contents are not a path the rename can carry by bytes, so the workspace rename refuses it with `conflict` before any
capture, naming the directory and the way out — commit it, move it out of the workspace, or exclude it, then retry.
Untracked files are preserved by path, mode and bytes; a nested repository is never recursed into.

Moving an adopted checkout is always cowshed-mediated (`mv <ws> <new-path>`): the image is detached, published at the
new path, and reattached — no unmount-and-remount gap at the user's path, no relink to maintain.

**Direct mount.** The checkout path _is_ the mountpoint, and a mountpoint cannot be renamed while it is mounted. This is
the real transaction: detach → rename the (now stub-carrying) mountpoint directory → rebind the substrate onto the new
path → re-attach. The rename is inverted on failure, so a refused move is indistinguishable from one never attempted.
Past the rename there is no way back worth taking: the record and the tree both name the destination, so a failure to
re-attach leaves a detached project at the right path, which the gateway's heal or `attach` mounts.

The record moves **before** the tree, in both layouts. Under direct mount the source path stops existing the instant the
rename lands, so a record still naming it would have nothing left to resolve, whereas a record naming the destination
becomes true as the rename completes. Recording ahead of the move is what makes the crash window recoverable in the
forward direction instead of the dead one.

Rebinding the substrate is a rebuild, not a mutation. The substrate config is shared by value — the substrate holds it
behind a reference every clone shares, and the execution host holds its own copy — so editing it in place would let an
outstanding clone observe a mount point derived from the new checkout path against a host still validating against the
old one. Config, host, and substrate are rebuilt together and swapped at once, under the project actor's exclusive
ownership, at the one moment nothing is mounted.

Refusals are self-guiding as everywhere else and fire before any mutation: for the workspace rename, an in-progress Git
operation, an untracked nested repository, or an in-flight job (a dirty tree is carried, see above); for the checkout
move, a relative destination, one that is occupied, one whose parent does not exist, and one that overlaps cowshed
storage or the current checkout. A dirty tree is _not_ a refusal for the checkout move — the working tree is carried
across untouched rather than republished, so there is nothing for a fence to protect.

A user who moves an adopted checkout by hand has not broken anything: `cowshed doctor` accepts a checkout path that
resolves to the workspace's volume root wherever it now sits, and `cowshed attach` converges the recorded path onto the
observed one. `mv <ws>` is the sanctioned front door; convergence is the safety net under it.

Convergence needs an observation the controller cannot make for itself. `current_dir(2)` reports main's mount rather
than the name the user reached it by, and it witnesses where the checkout actually lives. The CLI therefore carries its
invocation directory into `attach`, and convergence fires only when that observation is unambiguous: the path sits
inside main's mount, the checkout root above it resolves to that mount, that root lies outside cowshed's own storage —
so the mount path is never mistaken for the user's checkout — and it differs from what is recorded. The layout is
recorded from the same observation: a checkout that is a mount is direct mount by construction. An `attach` with no
observation converges nothing rather than guessing.

## The `main` remote

Every session workspace is minted with one remote, named `main`, whose URL is **main's canonical mount path**. The name
says what the remote is — the main workspace — so `git fetch main`, `git log main/main`, and `git diff main/main` read
as what they do, and an agent that knows nothing about cowshed guesses it correctly on the first try.

The URL is the canonical mount, never the recorded checkout path, and the two differ by checkout layout (see "Checkout
layout"): they are the same directory. Recording a separate checkout path would put a path outside the workspace's read
grants into its git config and would dangle the moment the checkout moves; the canonical mount is the path the substrate
owns, the path the grants already cover, and the path `cowshed mv` maintains. `mv` rewrites the remote in every mounted
workspace as part of the same transaction that moves the mount, for the same reason it rewrites the volume label. A
workspace that was detached while main moved has no volume to write into; `attach` re-runs the same idempotent
configuration when the workspace is next used, so the repair rides the front door a workspace already passes through
rather than needing a verb of its own. Both paths are the idempotent no-op the remote configuration is built to be.

A fresh mint starts from a clean slate: the workspace's `.git` arrives by CoW carrying every remote main had, network
URLs included, so mint **strips them all** before configuring its own. That is the "no remote URL ever exists inside a
sandbox" invariant, not a judgement about user intent — nothing in a just-cloned image is the user's yet.

Afterwards it is, and configuration **never clobbers a remote cowshed did not create**. Remote configuration is
idempotent and re-runs on reconciliation, against a repository an agent has been working in — where `cowshed repo`
mirrors and hand-added remotes are real, and `main` is a plausible name for one of them. Cowshed inspects before it
writes:

- No remote named `main`: create it.
- A remote named `main` whose URL is already the canonical mount: leave it. This is the idempotent re-run.
- A remote named `main` pointing anywhere else: it is not cowshed's. Leave it untouched, register cowshed's under
  `cowshed-main` instead, and print one `cowshed:` stderr line naming both. The workspace is fully functional; only the
  spelling in `next:` hints changes, and the hints print the name actually in force.

Cowshed never renames, retargets, or deletes a remote it did not create. The rule is the same one the secrets gate and
`.pre-cowshed` retention follow: cowshed adds, and lets the user remove.

**Reverse registration is optional and off by default.** `cowshed fork --register` (or `.cowshed.toml`
`[remotes] register-workspaces = true`) additionally adds a remote named `cowshed/<ws>` in the **main** workspace's
repository, pointing at the new workspace's canonical mount, so a human working in main can `git fetch cowshed/<ws>` and
review a workspace's branch without leaving their checkout. It is off by default because it is host-side state that
accumulates: a coordinator that mints and retires workspaces all day would grow a remote per workspace and leave the
stale ones behind on any crash between retire and cleanup.

The direction is the safe one. The reverse remote lets main **fetch from** a workspace; it never lets a workspace push
into main, which is the same pull-based hand-back `cowshed push`, autosave, and `land` all use, and for the same trust
reason — the sandboxed side is never the side that initiates a write.

`cowshed rm` drops the reverse registration as part of retirement, before the image is trashed, and `land` drops it at
the same point it prunes `refs/cowshed/<ws>/*`. A registration left behind by an interrupted retire is removed
idempotently by `cowshed gc` from the same revalidated retirement metadata that authorizes the rest of the cleanup — a
remote naming a mount path that no longer exists is never by itself authority to delete anything.

## `cowshed fork [<src>] <ws>` — create a workspace

The source defaults to `main`, or names any session. The clone carries the source exactly as captured, including
installed dependencies, build state, tracked edits and untracked files. Every fork runs this one mint path and switches
to its own `cowshed/<ws>` branch from the source's `HEAD`, never the source's branch. Grants start closed with a fresh
CA and platform endpoint identity; neither source grants nor endpoints carry over. No pool or pre-warming. The source
image remains live and mounted throughout capture.

1. `flock` the source's image lock. Flush its source volume and backing file for freshness without unmounting,
   detaching, sealing, or changing its mount flags. Source shells, editors and watchers continue to use it.
2. Open the source inode without following symlinks and descriptor-clone it to `sessions/<ws>.dmg`. RAW has no outer
   allocation map; the filesystem snapshot is crash-consistent, not an application-state transaction. Create the
   complete closed-baseline sibling sidecar before attach; allocate `portBlock` only on macOS and omit it on Linux.
3. Attach the exact clone once without mounting (`diskutil image attach --noMount`, flags per 01a_apfs.md §Mounts),
   verify the freshly probed, pinned APFS volume with `fsck_apfs -q`, then use `mount_apfs` to mount that exact volume
   at `<mount-root>/<owner>/<repo>/<ws>`. The mapping is guarded again after the host lease wait; verification precedes
   the first mount, and the held mount root/source is checked before publication. No combined mountpoint attach,
   stale-family selection, fallback or retry is allowed. RAW makes no fixed attach-latency guarantee. Before attach, the
   `first-write` step rewrites the clone's first block with the bytes it holds, which copies the source image's extent
   map (01a_apfs.md, "Clone cost follows extents, not size"). On a long-used main that costs tens of seconds, and under
   host load a container-wide metadata stall follows it, until `cowshed defrag main` rewrites main contiguously.
4. On attach, fsck, mount or identity failure, refuse publication with the actual error and release the destination
   attachment or report its cleanup refusal; never detach the live source as a fallback.
5. Rewrite `.cowshed/workspace.json` (`role: "workspace"`, `baseCommit` = source HEAD), mint a fresh `.cowshed/token`,
   mint a fresh per-workspace CA (private key controller-side next to the grant file; CA cert placed in-image as a trust
   anchor with the tool anchors wired — 04_sandbox.md/05_gateway.md), and complete platform wiring. On macOS, use the
   sidecar's allocated contiguous `portBlock` from the host partition of the macOS validation range; its size is
   recorded with it (04_sandbox.md). Fixture stores allocate from a reserved fixture partition of that validation range;
   the host never assigns there. The partition bounds are defined once in code. The gateway binds `base`, and
   `base+1 … base+size-1` are workspace service ports. On Linux, omit `portBlock`; after the dataset is mounted, create
   and bind-mount the per-incarnation Unix gateway socket and launch the trusted connector inside the workspace's
   private netns on `127.0.0.1:7644` (04_sandbox.md/05_gateway.md). Mark `<mount>/.envrc` direnv-trusted. In-image Bun,
   Cargo, Go, and proxy wiring uses the platform endpoint, and tool shims are placed at `.cowshed/bin/`; git reaches the
   network only through the gateway, like every other client (see "Remote code ingress").
6. Drop what the clone inherited that names a process in main rather than content, before any Git or daemon runs in the
   clone: every lock file in the workspace's own `.git` directory, and the Nx daemon's rendezvous directory
   (`.nx/workspace-data/d`, which every boundary of the checkout shares, 04_sandbox.md). Git takes `<file>.lock`
   exclusively, writes into it, and renames it over the file, so a clone taken while a writer in main held one carries a
   lock nobody in the workspace holds — the writer's rename lands in main — and every write to that file in the
   workspace would fail. A `.git` that is a file or a symlink names a Git directory in another tree, whose locks may be
   live, and is not walked. Then re-resolve inherited escaping symlinks. A relative symlink whose target climbs above
   the tree root (the entry `bun install` writes for a `link:` dependency, say) was computed against main's depth and
   lands somewhere else at the workspace's mount depth, so it is rewritten to the absolute path it named in main; one
   whose target does not exist in main either is refused by name rather than repointed at a guess. An absolute symlink
   whose target lies under the source checkout's root (a hook installer's `.git/hooks` entry, a package linker's link to
   a directory the checkout owns) names the source checkout's own files, which the workspace's sandbox cannot read and
   whose edits belong to another checkout; it is rewritten to the same path under the clone's root, spelled relative to
   the link, so it names the clone's own copy at every later depth. In-tree links and absolute links outside the source
   root are left alone. Before any private job runs, the mint's identity adds exactly the canonical sibling targets of
   the source's developer package links to its read grants (16_build_volumes.md, "Hash inputs"); it never adds an
   ancestor dependency store or the source's main root, and the source's owned-link marker is migrated first. Finding
   them reads every directory of the fresh volume, concurrently, and is the largest setup cost after the clone's first
   write. Then, inside the mount, under the workspace's closed sandbox: configure the `main` remote (see "The `main`
   remote") and `git switch -c cowshed/<name>` from the checked-out state. The `.git` directory arrived complete via CoW
   — the workspace is a standalone repository with **no linked-worktree registration and no back-references** into the
   host checkout, unless it was created with `--git-worktree` (see "Git-worktree workspaces").
7. Record the audit
   `ControllerCommitment::Fork(ForkCommitment { version, order, repo_id, source_incarnation, destination_incarnation })`;
   the new marker carries `lineage` = source incarnation followed by the source's own lineage, which authorizes the job
   records the clone inherited.
8. Print the mount path on stdout; guidance and `next:` hints on stderr.

Before the first slot, image, grant-reservation, or Git mutation, `fork` fsyncs its complete operation into the project
lifecycle-intent journal. The intent stays `Prepared` until the verb has something durable to recover: it is marked
`Mutating` once `--slot` is bound, or otherwise immediately before the staged clone writes its `PendingFence` sidecar. A
port-block claim is not durable: it belongs to the claiming process and lapses with it. A verb that fails while its
intent is still `Prepared` was refused (no free port block, an occupied slot) and puts back the journal record its
intent superseded before returning its error, so no later recovery can create a workspace whose caller was told it
failed. Startup first reconciles a pending destination against canonical inventory: an already-published incarnation
becomes the recorded result; an absent destination reruns the operation if its intent is `Mutating`, or if a
`PendingFence` image or a slot binding carries its name. Binaries before the `Mutating` mark journaled every create and
fork `Prepared`, so those durable facts, not the phase, decide a `Prepared` intent. One with neither mutated nothing;
startup discards it and names it on stderr instead of rerunning it. Reissuing the same call returns the recorded
incarnation instead of creating a second workspace or reporting a false conflict.

Flags: `--clean` resets tracked changes from `HEAD` and removes nonignored untracked files in the clone, with the
clone-owned registration ordering above for linked sources. Ignored paths stay warm. `--ref <rev>` implies `--clean` and
branches at the commit the revision names in the source repository, resolved before the intent is journaled. The public
API refuses a ref with `clean: false`; the CLI sets clean for a ref. Also `--browse`, `--slot <n>`, `--register` (see
"The `main` remote"), and `--git-worktree` (see "Git-worktree workspaces"). `<src>` replaces `--from`; `new` is not a
command or alias. The marker records `forkedFrom = <src>` and `lineage` as the source incarnation followed by its
ancestors, and the audit record is `ControllerCommitment::Fork`.

### Publication and crash recovery

The public fork journals the existing create intent, called the fork intent here. `mv`'s raw clone keeps its own intent
kind. Historical on-disk create/fork tags remain readable for recovery; they are storage formats, not CLI aliases.

`fork` publishes the destination before mounting it, and a crash resumes rather than rolls back. The lifecycle intent is
durable before storage mutation. Clone metadata is written at the canonical sidecar before the payload appears, in
publication state `PendingFence`, which keeps the destination out of ordinary enumeration until initialization and
post-callback validation finish. Activation is one atomic sidecar rewrite from `PendingFence` to `Active`; the mounted
attachment is then retained. The normal path has no staging mount, detach, image rename, canonical reattach, or remount.

On macOS, a complete canonical `PendingFence` image reserves its persisted `portBlock` even after its creator exits.
Allocation scans include those reservations without making the workspace visible or runnable. A dead creator's PID claim
may be reclaimed, but that never releases a block still owned by an image. Resuming `fork` reuses the stored grant
rather than allocating a temporary second block. Duplicate ownership remains an integrity refusal.

Every cowshed process that opens a project shares its intent journal. Each change re-reads and rewrites the journal
under an exclusive lock on `lifecycle-intents.json.lock`, so no process writes back a copy that predates another
process's intent. The process executing a workspace's lifecycle operation holds that workspace's intent lease
(`sessions/<name>.intent.lock`) until the verb returns; the kernel releases it when the process exits, however it exits.
Startup recovery acts only on unfinished intents whose lease it can take. An intent whose lease another process holds is
that process's running operation, not crash residue, and is never run a second time beside it. `cowshed doctor` runs no
startup recovery at all: it reports every unfinished intent and leaves it journaled.

A `PendingFence` clone that no unfinished intent names, and that no process is creating (its intent lease and its
image's lifecycle lock are both free), is abandoned: nothing will finish it, and because it was never published nothing
ever ran in it. `gc` and `doctor --repair` retire it as the create or fork its own metadata records, under the ordinary
pending-clone retirement checks, and `rm <name>` accepts it the same way. A retirement those checks refuse leaves the
clone in place and names the refusal. `gc` never deletes a `PendingFence` image directly.

| Kill window                                                      | Durable state                                                  | Recovery action and guard                                                                                                                                                                                   |
| ---------------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Before the `Mutating` mark                                       | `Prepared` intent only                                         | Nothing durable exists. A failing verb puts back the record its intent superseded; startup discards a leftover one and names it on stderr.                                                                  |
| After the mark, before pending metadata                          | `Mutating` intent, plus the slot binding with `--slot`         | Re-run create/fork normally. A `Prepared` intent whose slot is bound (an older binary, or a kill between binding and marking) is re-run the same way.                                                       |
| After sidecar, before complete clone                             | Sidecar only, or a failed clone                                | A clone failure reclaims both artifacts; startup recovery removes a sidecar-only record. No partial payload is admitted as resumable.                                                                       |
| After clone, before attach                                       | C + `PendingFence`                                             | Reuse the metadata incarnation and original operation identity; never clone over C.                                                                                                                         |
| After attach, before mount                                       | C + `PendingFence` + unmounted attachment                      | An exact inventory match is detached and settled, then the ordinary verified attach/fsck path repeats before mounting.                                                                                      |
| After mount, during image-local preparation                      | C + `PendingFence` + canonical kernel mount                    | Exact source-device and canonical mount flags reuse the existing attachment; ambiguous or foreign mounts fail closed. Rename and credential publication are idempotent.                                     |
| During the initializer                                           | The same pending mounted C, possibly with external Git effects | Cancellation and initializer errors preserve C and the mount. The retry receives explicit `stage.resuming` authority; the branch and worktree state machines continue only their exact cowshed-owned state. |
| After the marker write                                           | The same pending mounted C                                     | An already-current marker is validated rather than rewritten, so lineage never duplicates itself.                                                                                                           |
| After the callback, before or during activation                  | Pending or `Active` sidecar (atomic rewrite)                   | Pending reruns the callback and validation; `Active` is listed as current. No intermediate publication state exists.                                                                                        |
| After activation, before downstream effects or intent completion | `Active` C + pending lifecycle intent                          | Startup recovery reruns optional main registration and append-safe commitments, then records exact completion. Slot binding is retained across errors, and rebinding the exact owner is a no-op.            |
| After completion                                                 | `Active` C + completed intent                                  | Reissuing returns the recorded incarnation; supervisor startup is start-or-reuse.                                                                                                                           |

Every initializer step can run again:

| Step                                                              | Replay rule                                                                                                                            |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| inherited Git locks and daemon discard                            | Not-found is success; deletion is deterministic.                                                                                       |
| environment hook                                                  | Check-first, with an append-open recheck.                                                                                              |
| inherited remotes and links, local `main` remote                  | An empty loop or a deterministic rewrite, plus ownership-checked remote configuration.                                                 |
| standalone branch                                                 | A fresh preparation refuses a collision; a storage-authorized resume reuses and checks out only `cowshed/<name>`.                      |
| linked-worktree registration                                      | Exact admin and pointer states continue through staging-pointer relocation and `worktree repair`; mixed or foreign states fail closed. |
| marker                                                            | An exact current marker validates and skips the rewrite.                                                                               |
| credentials                                                       | Atomically overwritten while still unpublished.                                                                                        |
| slot                                                              | Binding the same workspace to the same slot is idempotent, and the binding stays owned by the pending intent.                          |
| optional registration, commitments, intent completion, supervisor | Set or check first, append-safe telemetry, overwrite-safe completion, start-or-reuse.                                                  |

**Rollback was rejected.** Initialization changes state outside the image: the `cowshed/<ws>` branch and, for a
git-worktree workspace, its registration live in main's repository. Deleting the image of a crashed create orphans them,
compensating is itself crashable, and deleting a branch that acquired commits between the crash and the reclaim loses
work. Rolling back would also need a fence for the corpse and a reclaim rule anyway, and it has a window in which a
crash after a successful initialization deletes complete work. Resume never deletes state a user can reach.

**Mounting the staging image and renaming it under the live mount was rejected.** It renames an attached image's backing
file, after which the attachment inventory, keyed by the path the image was opened with, reads a mounted image as
detached. A crash during initialization would also leave a staging image mounted at the canonical mountpoint, which the
staging-mount collector does not retire, so the retry would find its own mountpoint busy.

## Git-worktree workspaces: `cowshed fork --git-worktree`

A workspace is a standalone repository by default. `--git-worktree` makes it a **git-worktree workspace** instead: a
registered linked worktree of the main workspace's repository, sharing main's object store and ref namespace rather than
owning a copy of them.

Everything expensive still arrives by CoW. The image is cloned from main exactly as always, so `node_modules`, build
outputs, and the warm in-image caches cost the same ~2 ms they always did; only git state is replaced afterwards. Mint
gains three steps between attach and publication:

1. Discard the cloned `.git` **directory** in the image. It is a complete copy of main's repository and a linked
   worktree must not carry one — two registrations claiming the same worktree id is precisely the failure mode that
   makes cloning an already-linked worktree wrong (see the prototype report).
2. Register the worktree against main's repository and plant the pointer file, using **main's canonical mount** as the
   repository path, which is the checkout path. The registration must name the real mount path rather than any alias for
   the same reason the `main` remote does — a registration recorded through an alias breaks when it moves, and
   `git worktree repair` would then have two plausible paths and no way to choose. Because the tree is already present
   from the clone, nothing is checked out: cowshed registers with `--no-checkout`, relocates the pointer onto the mount
   root, and reconciles both directions with `git -C <main-canonical-mount> worktree repair <mount>`, which is the
   primitive git provides for exactly this two-way pointer fixup. `git worktree add` insists on creating the path it
   registers, so the registration is taken out through a staging directory inside the image and the pointer file is
   moved up to the mount root afterwards; the staging directory's name is the worktree id git derives from it, which is
   why it is the workspace name and not anything mount-derived — a `--slot` mount does not carry the name, and
   retirement has to find the registration from the name alone.
3. Refill the index from `HEAD` with a mixed reset, then `git switch -c cowshed/<ws>` as usual. `--no-checkout` leaves
   the index empty. Refilling it preserves working-tree edits and untracked files in a carrying fork, but not
   staged-only state. `--clean` discards edits before registration when the clone has its own `.git` directory, and
   after registration through the clone-owned index when it inherited a source pointer (see the safe-to-clone-main
   invariant). The branch is created in main's ref namespace and is immediately visible there, with no fetch and no
   `main` remote: the object store is already shared. `cowshed rebase` therefore defaults to `main` itself and fetches
   nothing first. A lane does fetch: `rebase --into` a lane and `land --into` a lane base move work by a local fetch.
   While main's config includes a file of main's checkout, a git-worktree workspace's controller Git executes only Git
   (below), and a local fetch runs `upload-pack` through a shell, so such a fetch is refused before Git runs, naming the
   include and the rule: move the include into main's `.git` or out of main's config, or route the work through a
   standalone lane.

A workspace's mode is inherited. A fork or `mv` of a git-worktree source is re-registered under its own id because its
clone contains only the source's pointer file. `--register` is refused for any git-worktree fork: main already holds its
branch.

Main's config is shared along with its objects, so the network remotes in it are visible from inside a git-worktree
workspace — cowshed cannot strip them without editing the user's own repository, and does not. The invariant that holds
is the enforced one: the sandbox has no egress those URLs could reach, and cowshed still never _writes_ a remote URL
into a repository a workspace can see.

**Main must be mounted**, at mint and at every subsequent attach and exec. The gitdir lives outside the workspace's
volume, so with main detached the workspace has files and no repository — `git status` fails, and so does everything
built on it. Cowshed checks before it acts and refuses with `next: cowshed attach main` rather than handing back a mount
whose git is broken. This is the mode's defining cost, and it is why it is not the default: a standalone workspace is
usable with main detached, unmounted, or being restored, and a git-worktree workspace is not.

It is also a hole in the isolation, stated plainly. The sandbox needs read and write into
`<main-canonical-mount>/.git/worktrees/<ws>` and into main's object store — a committing agent writes objects into the
user's own repository. The profile carries that hole as a distinct, controller-owned carve-back on
`<main-canonical-mount>/.git` and nothing wider, emitted after every deny that would otherwise close it (04_sandbox.md).
It cannot be an ordinary read/write grant: grants are refused outright when they intersect a protected path, and main's
mount is one — the denied project root under direct mount. Main's working tree stays as unreachable as any other
workspace's, and a workspace that did not ask for `--git-worktree` never has any of it.

The controller's own Git for such a workspace (mint, attach, `rm`, `gc`, `rebase`, `land`) reads main's shared config
through that carve-back, and Git refuses to run when a file the config includes cannot be read: a relative include such
as `../tooling/workspace.gitconfig` lands in main's working tree. Every job of the workspace can write that config, so
the controller resolves its `include.path` and `includeIf.*.path` targets itself, transitively, and holds each to the
narrowest reading. `~/` is the HOME the controller's Git runs with, inside the workspace. A target in the workspace or
in main's `.git` is readable already. A regular config file inside main's checkout is granted to that Git, by itself,
read without following a final symlink, without blocking on a FIFO, and only up to 1 MiB. Any other target — outside
main's checkout, a device, a FIFO, past 64 included files — refuses the command by name; an include outside main stays
denied, as it always was.

A git-worktree workspace's controller Git runs no repository driver. Every `filter.<name>` clean, smudge and process
command is emptied, as hooks already are; every `merge.<name>.driver` and `diff.<name>.command` is `false`; every
`diff.<name>.textconv` is the identity. A merge or rebase that needs a driver — a pin-merging `merge=<driver>`
attribute, say — therefore conflicts there instead of running it. Those overrides name the drivers the config holds when
the command is built, which is not a boundary: a job can add one under a new name before the command runs, and
`gpg.program`, `core.sshCommand` and the like are not drivers at all. So while that Git holds a grant of main's
checkout, its sandbox executes nothing but Git's own executables — the developer directory's `usr/bin/git` and
`usr/libexec/git-core` — and any program a config key names fails closed, a shell included. A standalone workspace's
controller Git holds no such grant and keeps running the repository's drivers (04_sandbox.md, "Controller Git"). A job's
own Git inside a git-worktree workspace keeps only the `.git` carve-back, so an include into main's checkout still fails
there; widening the job profile is not part of this.

**Checkpoint and restore refuse** on a git-worktree workspace, exit 4, with `next:` hints. The image is not
self-contained: a checkpoint clone would capture a tree whose history lives in a repository it does not include, so the
clone is not a restorable state — it is half a repository. Restoring one is worse than useless, because the fresh
incarnation would resurrect a registration for a worktree id that main has since pruned and quietly claim a branch
another workspace may now hold. The hints name the two honest substitutes: commit and `cowshed land` to keep the work,
or `cowshed fork` a standalone workspace when checkpointing is what is actually wanted. Cowshed refuses rather than
checkpointing something that cannot be restored.

`cowshed rm` drops the registration as part of retirement, in the same host-side-state-first step that drops a reverse
remote: remove the worktree registration from main's repository, then trash the image. Unregistering is the removal of
that worktree's administrative directory and nothing else, because both commands that look more obvious are wrong here.
`git worktree remove` deletes the working tree at the registered path — the workspace's own files, which retirement
trashes as an image and `gc` may find already gone — and refuses on dirty state that `rm --force` has already accepted.
`git worktree prune` unregisters _every_ worktree whose path is missing, and a merely detached cowshed workspace looks
exactly like that. A registration orphaned by an interrupted retire is removed idempotently by `cowshed gc` from
revalidated retirement metadata, before the image that carries that metadata is deleted — never from the mere
observation that a registered worktree's path is not mounted.

The mode is recorded store-side, in the workspace's detached sidecar, for the same reason: every decision it drives —
refusing checkpoint, requiring main mounted, pruning the registration at retirement — has to be made while the workspace
is detached and its mount can say nothing. A standalone workspace's sidecar omits the field.

`cowshed mv` repairs registrations. Moving main under direct mount moves the gitdir every git-worktree workspace points
at, and moving a workspace moves the path main's registration records; both directions are repaired inside the same
transaction that moves the mount, so there is no window in which a registration names a path nothing is mounted on.
`cowshed land` needs no special case: it fast-forwards a checked-out target only through main's own checkout,
compares-and-swaps a target nothing has checked out, and refuses a target held by any linked worktree, a git-worktree
workspace or a foreign one: git never checks one branch out in two worktrees, so that worktree would be left with a
stale index and files.

### Why clone is the default

Not storage. CoW makes a complete `.git` copy free — that is the whole premise of the substrate — so the classic
argument for linked worktrees, that they avoid duplicating the object store, buys nothing here. It was never the reason
and is not a reason now.

The reasons are both about blast radius:

**Crash isolation of the object store.** A shared object store is one repository that every worktree writes into, main
included. A hard crash mid-write, a killed `git gc`, a corrupted pack, an over-eager `--prune` — in clone mode each
lands inside one disposable image, and the cure is `cowshed rm` and a fresh `cowshed fork`. In worktree mode it lands in
the user's own repository, and the blast radius is every workspace plus main. Cowshed's entire posture is that a
workspace is cheap and destroyable; that only holds while destroying one cannot take anything else with it.

**History rewrites want isolation.** Workspaces exist to be rebased, replayed, and thrown away. `cowshed rebase --fresh`
replays a branch onto a new base and destroys the old clone; agents rebase, amend, reset, and expire reflogs freely
because nothing outside the image can notice. Doing that inside the user's object store means an agent's experimental
rewrite shares a reflog and a loose-object pool with the human's work. A workspace whose job is to rewrite history
should not be rewriting it where the user lives.

`--git-worktree` exists for the cases where the shared namespace is the point: tooling that assumes one repository,
`git worktree list` as the inventory, and reviewing a workspace's commits from main with no fetch step at all. It is a
deliberate trade of isolation for immediacy, taken per workspace, and never by default.

## `cowshed checkpoint <ws> [label]` / `cowshed restore <ws> <label>`

- Both verbs refuse a git-worktree workspace before any quota is read or any barrier is taken (exit 4, `next:` hints —
  see "Git-worktree workspaces"). Its history lives outside the image, so there is no self-contained state to capture
  and nothing coherent to restore.
- Before pausing the supervisor or cloning, cowshed reads the target workspace's coordinator-owned `CheckpointQuota` and
  authoritative substrate stats. The projected count is its existing published checkpoint count plus one. The projected
  bytes are its existing published checkpoint allocated bytes (pinned and automatic) plus the active image's
  conservative allocated-byte charge. Other workspaces are isolated from this cap. Exact boundaries are allowed; an
  over-boundary projection is `Conflict` with no checkpoint image, fact, detached-metadata, or barrier publication.
- Checkpoint is a supervisor/filesystem barrier. Under the lifecycle lock, cowshed pauses admissions and artifact
  writes, promotes every running memory-only stream prefix, and fsyncs protected files. It then appends and fsyncs
  exactly
  `ProtectedRecord::CheckpointManifest(CheckpointManifestRecord { version, repo_id, origin_incarnation, barrier_id, visible_jobs, records_sha256 })`.
  `barrier_id` is positive and monotonic within `origin_incarnation`; `records_sha256` hashes the complete
  protected-record stream prefix immediately before the manifest batch. `visible_jobs` contains
  `VisibleJobCommitment { workspace_incarnation, job_id, state, stdout, stderr }`; each stream commitment is
  `{storage_kind, bytes, sha256, protected_path}` with `storage_kind` exactly
  `captured-inline|captured-file|redirect-inline|redirect-file` and `protected_path` present iff file.
- With writes still quiesced, cowshed syncs for filesystem freshness and clones the image. The controller records the
  audit
  `ControllerCommitment::Checkpoint(CheckpointCommitment { version, order, repo_id, origin_incarnation, checkpoint_id, barrier_id, manifest_batch_sha256 })`,
  then resumes. `manifest_batch_sha256` is the recovery frame's digest for the complete manifest batch. The manifest
  defines every checkpoint-resident byte: terminal inline data is in a prior protected Job record covered by
  `records_sha256`; every running prefix is an fsynced protected file. Complete batches/sealed files are immutable;
  recovery may discard only an incomplete trailing frame while retaining and reporting its `batch_sha256`. Committed
  digest/count mismatch is `Integrity`.
- Restore follows the substrate transaction in 09_substrates.md: lock and revalidate the old incarnation/revision; stop
  admissions and drain the supervisor and, on Linux, the connector; detach; prepare and verify a non-enumerated
  same-format clone; verify its protected checkpoint manifest is complete and self-consistent (the manifest's own
  `records_sha256` and batch digests); mint and write the fresh `workspaceIncarnation` and its `lineage` (the
  checkpoint's origin incarnation, then that image's own ancestors) **before** minting its fresh token; atomically swap
  the clone into the canonical name; attach and validate; on Linux create the fresh per-incarnation socket and
  connector. At the same atomic publication boundary, publish detached metadata and switch gateway acceptance from the
  old endpoint/token to the new; record the audit
  `ControllerCommitment::Restore(RestoreCommitment { version, order, repo_id, source_checkpoint, source_incarnation, destination_incarnation })`;
  only then admit the new supervisor. A missing artifact, hash/count/batch-digest mismatch, or malformed complete frame
  fails restore as `Integrity`; cowshed never chooses the newer-looking side. The logical workspace identity, primary
  `repo_id`, CA, and grant binding carry through; a macOS `portBlock` carries through only when present, while Linux has
  none. The displaced image becomes `checkpoints/<ws>/pre-restore-<destination_incarnation>.<ext>` with its original
  extension, so restore is undoable. The checkpoint source incarnation, the replaced active incarnation, and the fresh
  destination incarnation are distinct roles; on repeated restore the checkpoint source can differ from the replaced
  active generation. Before publication, failure restores the displaced workspace and old token; after the incarnation
  fence is published, recovery completes forward and never exposes both tokens. Copied job records retain the
  incarnation that produced them, while the new incarnation allocates workspace-local monotonic numeric job IDs above
  every inherited allocation. Restore restart recovery reads only substrate facts: the canonical/pending detached
  metadata, the destination-keyed displaced image/grant/CA sidecars, and the exact sibling `.restore.json` recovery fact
  described in 01a_apfs.md, "Restore publication custody". Before durable pending-metadata publication it restores the
  old generation; afterward it leaves the new generation pending for idempotent activation (the restore's audit record
  is emitted again on the way — the sink is telemetry, so a repeat costs nothing it protects). Repeating startup
  recovery and activation cleanup has the same result. The project runtime persists no restore fence, journal, or
  `.restore-fences` path.

- `cowshed gc` retains every pinned checkpoint, every checkpoint younger than 14 days, and always the newest five per
  workspace. A supplied label and `--keep` both create explicit pins; only an explicit unpin makes them eligible.
  `CheckpointOptions.keep` carries the same pin request for programmatic callers. `WorkspaceInfo.checkpoints` always
  projects the canonical `{label, revision, pinned}` facts (an empty array when none exist), so detached workspaces do
  not need a mount or cached marker to report their retry points.

### Checkpoint before writer handoff

Before write responsibility passes from one writer to another, the current writer creates a checkpoint after stopping or
quiescing its workspace writes. The checkpoint barrier also makes every supervisor-resident captured byte durable and
publishes the protected manifest described above (and records the checkpoint audit record). That checkpoint is the
recovery boundary for the handoff and includes the complete live workspace state, including committed and uncommitted
files, plus exactly the job evidence committed by its manifest. The next writer may receive a fresh capability for the
same attached, writable workspace, or the handoff may create a live copy-on-write fork from that workspace. A fork
receives its own identity and closed-baseline grants as described above; the source and its checkpoint remain
recoverable until the fork is accepted or independently preserved. A failed writer can therefore be replaced on the same
workspace, or its attempt can be abandoned in favor of the validated checkpoint or an unaffected fork, without
conflating handoff with publication or retirement.

## `cowshed push <ws> [--branch <name>]`

Preserves a completed workspace branch in the **main workspace's standalone repository/object store**. This is a local,
pull-based Git operation; it does not switch or update the checked-out Git `main` branch and does not modify the main
workspace's index or working tree. The default source is the workspace's `refs/heads/cowshed/<ws>` branch. A validated
`--branch` selects another local source branch. The destination is the non-checked-out namespaced ref
`refs/cowshed/<ws>/heads/<branch>` in the main workspace repository.

The controller reads from the source workspace using host-side Git with the main workspace repository as the receiving
repository, `--no-write-fetch-head`, and no checkout or merge. It fetches the selected source into a temporary ref,
verifies the fetched object ID, then atomically installs that exact object ID at the destination ref and removes the
temporary ref. The receiving command never uses the source workspace's Git configuration or hooks. Updating an existing
destination is deliberate preservation of a newer snapshot, not a working-tree operation.

`push` accepts three independent optional compare-and-swap preconditions: the expected source workspace incarnation, the
expected source branch head object ID, and the expected destination ref head (either a specific object ID or missing).
Under the destination-ref lock, cowshed validates every supplied expectation against authoritative host metadata and the
fetched source. Any mismatch returns `Conflict`, reports which expectation moved, leaves the destination ref unchanged,
and retains the source workspace. Omitting an expectation requests the corresponding unconditional behavior; callers
that may race or retry should supply all three.

Success returns the source object ID and destination ref and guarantees that the destination resolves to that exact
object ID. Once every unique commit that must survive is reachable from this durable namespaced ref (or another
non-temporary ref in the main workspace repository), the source workspace may be retired. Merely transferring objects,
leaving them dangling, or updating `FETCH_HEAD` is not preservation. Remote forge publication, pull-request mutation,
workflow policy, and scheduling are outside Cowshed; another trusted host-side component may publish the preserved ref
without changing these local semantics.

## Autosave

A per-project launchd user agent (installed at adopt; systemd user timer on Linux) snapshots every _attached_ workspace
into the host repository on a 10-minute cadence (`.cowshed.toml` `[autosave] interval`). The mechanism is a host-side
fetch — the workspace branch plus `+HEAD:refs/cowshed/<ws>/wip` — the same pull-based direction as `cowshed push`, for
the same trust reason. `refs/cowshed/…` (singular) is the canonical namespace; `land` prunes a workspace's refs at
retire. Each snapshot is gated by the secrets delta-scan (see "Secrets enforcement"): a finding skips that workspace's
snapshot with one `cowshed:` warning and never blocks work. Uncommitted work between autosaves is the only loss window —
images are excluded from backup by design (01a_apfs.md).

## Remote code ingress: `cowshed repo`

A workspace's own remote is the `main` remote (main's canonical mount). Git reaches the network only as every other
client does — through the gateway, to a host the workspace holds an intercepted egress grant for — and only to fetch: an
intercept grant admits git's smart-HTTP `git-upload-pack` and refuses `git-receive-pack` (05_gateway.md "Egress modes").
There is no git endpoint on the gateway data plane and no credential helper inside any workspace.

Dependency fetches by URL (Cargo git dependencies, uv git sources, `git ls-remote`) resolve through **fetch routes**: a
controller-generated, ignored `.cowshed/git-fetch.inc` that every child includes, rewriting each bound identity URL of
an adopted project onto that project's local object store when the workspace can read it. Routes come only from
repository bindings, never from a checkout's remotes, because a workspace can write its own `.git/config` and a remote
it added must not redirect another repository's URL. A URL routes to exactly one clone; git matches rewrites as literal
prefixes, so an SSH URL routes both as recorded (with its login) and without the login. Routing is not a grant — the
fetch still runs under the workspace's own read boundary. A binding starts with the adopted remote;
`cowshed identity add <remote>` (06_cli.md) binds another remote main's checkout configures, refused when another
adopted project binds that URL. Every bound remote stays under its recorded name: an open refuses a binding whose remote
was removed or now names another repository, and follows one that moved servers.

- **`cowshed repo mirror <url>`** — a control-plane RPC, not workspace git. The gateway checks the workspace's repo
  grants (`cowshed grant <ws> --repo github.com/org/*` — repo-scoped, finer than host egress), executes the fetch itself
  with Keychain-held credentials into a bare mirror it owns under cowshed's user cache directory,
  `~/Library/Caches/dev.cowshed/repo-mirrors/` (Linux `$XDG_CACHE_HOME/cowshed/repo-mirrors/`), writes one audit line,
  and returns the mirror path on stdout. Mirrors are created by the gateway with its own config — no agent-writable git
  config is ever in the loop — and are fetch-only and gateway-only: that directory is cowshed controller state, which no
  sandbox can read or write (01_storage.md, 01a_apfs.md, 03_caches.md, 05_gateway.md).
- **`cowshed repo clone <url> [dir]`** — sugar: mirror, then a host-side `git clone --dissociate <mirror>` into the
  workspace, so the clone holds its own objects and the sandbox never reads the mirror.

The project's own origin is just another mirror — mirroring it covers "the agent needs `refs/pull/123/head`". GitHub API
operations (creating PRs, issues) are coordinator work in v1: they require credentials and therefore live outside the
sandbox by construction.

## Artifact egress: `cowshed sim export` / `cowshed app export`

The outbound sibling of `repo` ingress, for posture B (14_nix.md). `cowshed sim export <ws> [artifact]` copies a built
iOS `.app` to the one-way drop dir (`<shared-drop-root>/<owner>/<repo>/`, using the primary path-safe `repo_id`), where
the personal session installs it into the human's simulator; `cowshed app export <ws> [artifact]` is the Mac-target
sibling. Either way an immutable artifact crosses the uid boundary, never a live tree; semantics, gating, and the
`--sim` grant class live in 14_nix.md / 04_sandbox.md / 05_gateway.md. The lane-3 counterpart `cowshed app promote` is a
**personal-session human verb** (it installs a promoted build into `~/Applications`), documented in 14_nix.md — not a
workspace or sandbox operation and not agent-invokable.

## Return to an integration branch: `cowshed rebase` and `cowshed land`

Workspaces are born from the main workspace's Git state and return to a controller-selected integration branch. `fork`
is the first half of the lifecycle; `rebase` and `land` are the second. Between them, autosave refs (fetched host-side
to `refs/cowshed/<ws>/wip` — see "Autosave") remain the crash-safety net. Cowshed provides the primitives; retry,
scheduling, review, remote publication, and branch-selection policy are coordinator work (12_mcp.md), deliberately not
baked in.

### `cowshed rebase <ws> [--onto <branch-or-ref> | --into <lane>]`

Brings a workspace's branch up to the selected host revision. The default is `main`; `--onto` accepts a validated local
branch or fully qualified ref in the main workspace repository. An object ID may be used as an immutable replay base,
but `land` still requires a named branch as its fast-forward target. `--into <lane>` selects what the workspace lands
into instead — the branch the lane base `<lane>` has checked out, fetched from the lane base's mount into the workspace
as `refs/cowshed/targets/<lane>/<branch>` (see "Lanes" below). `--onto` and `--into` are two destinations for one rebase
and are refused together (exit 2).

- Default mode: fetch the selected host revision inside the workspace, then rebase the `cowshed/<name>` branch onto that
  exact object ID. The whole step runs **inside the workspace sandbox** — the fetch from the main workspace mount is a
  read (covered by the baseline), and every write lands in the workspace's own repository. A dirty tree is refused
  before the rebase starts (exit 4, commit or discard first): the rebase runs with `--no-autostash` whatever the
  repository's config says, because an autostash re-apply that conflicts leaves unmerged paths and a parked stash yet
  exits 0. Conflicts abort the rebase (`git rebase --abort`), leave the workspace exactly as it was, and exit 4 with
  `next:` hints naming the conflicted paths and the by-hand replay. A rebase that did not conflict then carries its
  target's Nx cache entries into the workspace's build volume (16_build_volumes.md, "Rebase carry").
- `--fresh`: divergence-shedding rebase. Create a new clone from the selected current host revision, replay the branch
  onto it (`git format-patch`/`am`, or cherry-pick range), then **transplant identity** — the new clone inherits the
  workspace's name, canonical mount path, token, and grant-file binding; the old clone is destroyed (11_shell.md
  teardown ordering). This drops accumulated substrate divergence (ZFS origin-snapshot pin, APFS image growth —
  01_storage.md, 09_substrates.md) and re-warms in-image caches to the selected base. A replay conflict leaves the
  **original** workspace untouched and discards the half-built fresh clone (exit 4). Refuses (exit 4) when the workspace
  has uncommitted changes — replay carries commits only; commit or discard first. `--fresh` is what a coordinator runs
  against a long-lived workspace that `cowshed du` shows has drifted far from its integration base.

### `cowshed land <ws> [--into <lane>] [--target <branch>] [--check <cmd>]`

The full born-from-host-return-to-host close-out, as one primitive. It lands into main, or with `--into <lane>` into
that lane base (see "Lanes" below); "the target" below is whichever one it is. The target branch defaults to the branch
the target's checkout has checked out (`main` for main); a detached checkout with no `--target` is refused with exit 4
before anything runs. How the branch moves depends only on checkout state, read again under the repository lock just
before the update, because the check can run for minutes while the checkout is switched (step 3).

Land does not rebase. It lands the head the caller validated and named (`--expected-source-head`), and an implicit
rebase would land a head nobody named whenever main had moved. A workspace whose base main has moved past fails the
fast-forward with exit 4 and a hint naming `cowshed rebase <ws>`; rebasing and re-running the gates is the caller's
step.

The SmoothBricks host land gate refuses a source containing pre-rewrite main `e9e596563` before rebasing and names the
ordinary remedy `git rebase --onto <new-main> e9e596563`; it never replays that old range.

1. **Refuse a dirty tree** (exit 4), read exactly as `rm` reads one (a tracked change, or an untracked file that is not
   hidden, not ignored by main's current rules, and not held by main's checkout with the same content). The check runs
   in the working tree but only the commit lands, so uncommitted work would be validated without landing, and would then
   refuse the retire after main had already moved.
2. **Validate**: run the check _inside the sandbox_ — `--check <cmd>` if given, else no validation with one honest
   `cowshed:` stderr line saying so. Non-zero check → exit 4, workspace intact, output captured as a job (11_shell.md)
   for diagnosis. The check is an ordinary sandboxed exec and gets exactly the environment one gets (04_sandbox.md): the
   workspace's optional activated shell over cowshed's own job contract — the bootstrap `PATH` of the
   `direnv`/`devenv`/`nix` packages and system directories, plus a private `HOME`. A verify command that needs more
   needs it wired into the workspace, not leaked from the caller's shell; the check runs where the work was done, and a
   check that only passes in the coordinator's environment has not validated the workspace.

   Write checks as **bare commands** — `just verify`, `cargo test --workspace`. The sandbox `PATH` already _is_ the
   project's pinned toolchain, resolved to store paths, so wrapping a check in `devenv shell --` or a direnv
   re-evaluation asks for exactly the two things the sandbox deliberately withholds: the caller's `HOME` and a fresh
   evaluation. The wrapper fails where the command inside it would have passed. This is also what makes coordinator and
   worker verdicts identical rather than merely similar: `cowshed exec <ws> -- <cmd>` and `land --check <cmd>` are the
   same sandboxed exec, so a worker that fixes its own failures before hand-off has already run the check the
   coordinator will run.

3. **Fast-forward the target branch under its repository lock**: **fetch** the workspace's branch from its mount into
   the target's repository — a workspace is a standalone repository whose commits the target has never seen, so without
   this the merge resolves an object that does not exist and fails as "not something we can merge" — then revalidate the
   source workspace incarnation, source head, and expected target head, and require a fast-forward. The fetch is also
   the revalidation: what arrives must be the head the check validated, and a workspace that advanced in between is a
   conflict rather than an unchecked land. The fetched branch lands in the workspace's own preservation namespace,
   `refs/cowshed/<ws>/heads/<branch>`, which retirement prunes.

   **Land has no branch-name contract.** It fetches whatever branch the workspace has checked out — `cowshed/<ws>` is
   what `fork` creates, but an agent may have made `wt/<ws>` or anything else, and land neither assumes nor requires the
   minted name. Only the resolved head is load-bearing. A workspace with no checked-out branch is a conflict naming the
   fix.

   How the update is materialized depends only on checkout state:
   - If the target branch is checked out in the target's checkout, the fast-forward runs **through that checkout**: its
     branch ref, `HEAD`, index, and working tree all advance to the validated source tree. Cowshed never updates only a
     hidden or non-checked-out ref while leaving the visible target stale. The target must be clean enough for the
     update or `land` refuses with exit 4 and a `next:` hint to commit or stash.
   - If the target branch is not checked out, land atomically compares-and-swaps `refs/heads/<target>` from the expected
     target head to the validated source head, leaving the checkout's checked-out branch, `HEAD`, index and working tree
     untouched. A failed compare-and-swap is a `Conflict` and changes nothing.
   - If the target branch is checked out by any linked worktree, a git-worktree workspace or a foreign one, land refuses
     rather than leave that worktree's index and files stale.

   If the target advanced during validation, any expected head changed, or the update is not a fast-forward, return
   `Conflict`, leave both source and target intact, and report the moved value. Cowshed never retries against a new base
   internally; the coordinator decides whether to rebase and re-run checks.

   Every rebase and land fence reports its refusal as data as well as prose: the `Conflict` carries a `FenceRefusal`
   (07_api.md "Errors") naming the fence and what it observed — the moved incarnation, source, destination or target
   head, the non-fast-forward target tip, the branch the target has checked out, the dirty paths of either tree, or the
   head a conflicted replay was rolled back to — so a coordinator types the outcome without reading either repository
   again.

4. **Freeze the seed, adopt the build volume and restore checked outputs**: every land refreshes the target's seed by
   sealing and cloning the landing workspace's warm build volume before the target adopts it by renaming its
   `.cowshed/build` link. It restores only the attributed Nx task graphs of the checks already completed in the landing
   workspace, from the adopted local cache. A miss or uncacheable task is reported without executing it; check commands
   never run again in the target. 16_build_volumes.md, Land step 7, owns restoration, writer-quiescence skips and
   collection; every seeded/unseeded and adopted/skipped outcome is durable (13_telemetry.md).

5. **Retire**: only after the target branch and, when checked out, its visible working state resolve to the validated
   source head, destroy the workspace (supervisor tree first — 11_shell.md) and prune its `refs/cowshed/<ws>/*`
   preservation refs on the host. The retire gate is `rm`'s, measured against the branch the workspace just landed on,
   in the target's repository: a lane unit whose commits its lane base holds retires, though main does not hold them
   yet. A retire refused at this point (the check left work behind, say) keeps the workspace and exits with the refusal,
   but its message starts with the landed head and target: the target has moved, so the next step is `cowshed rm`, not
   another land.

`--no-retire` keeps the workspace after a successful land (for a coordinator that wants to reuse it via
`rebase --fresh`). `--push-only` performs steps 1–2 and installs the validated head in the workspace's durable
`refs/cowshed/<ws>/heads/<branch>` preservation ref without advancing the target branch, for review-gated flows. It
returns `LandReport::Delivered(PushReport)` before build-volume adoption, land commitments, retirement or collection,
even with the default `retire: true`. An ordinary land returns `LandReport::Landed(LandedReport)`; its existing landed
fields describe only an actual target advance. The canonical JSON report uses the flat `kind: delivered | landed` tag.

A target fast-forwarded through plain Git instead is an **external land**: it forfeits carry, output restoration and
adoption. The next fork (including `--ref` naming the target's current tree) automatically captures a live workspace at
exactly that tree as the target's seed, under writer exclusion; no manual seed-refresh or warm command exists. `doctor`
reports `seed-tree-behind` and `target-moved-outside-land` when the recorded volumes do not match the tree.

### Lanes: `fork` and `--into`

A lane is a line of work that reaches main as one change but is built by several workspaces. Its base is an ordinary
workspace, `cowshed fork <lane>` (a clone of main). Its units are `cowshed fork <lane> <unit>`. A unit rebases onto
(`cowshed rebase <unit> --into <lane>`) and lands into (`cowshed land <unit> --into <lane>`) the lane base, then is
removed. The lane completes with the ordinary close-out of its base: `cowshed rebase <lane>` and `cowshed land <lane>`
fast-forward main and remove the lane base.

The caller (an external coordinator) tracks the tree; cowshed stores nothing about lanes. `--into` names a workspace,
and every lane rule is read off that workspace at the moment of the call: its checked-out branch is the target, its
repository is where the unit's commits are fetched and fast-forwarded, and its branch is what the retire measures
against. A standalone `cowshed rm` of a unit still measures against main.

`--into` refuses the unit itself (exit 2), and `--into main` is the default. In the API the lane is a `WorkspaceRef`
rather than a name: the reference carries the incarnation it was resolved at, and land and rebase refuse (exit 4, naming
the recreation) when the lane base's current incarnation differs — a lane base removed and recreated under the same name
is another workspace, and a unit's work never lands into it by name alone. The CLI resolves `--into` to a reference
once, when it runs.

Nothing in the model depends on how a clone is made: a lane base is a workspace and a unit is a clone of it, which is a
clone of a snapshot of the lane base on ZFS as it is a clone of its image on APFS.

## `cowshed rm <ws>` — perceived-instant deletion

1. Refuse (exit 4) unless the workspace's HEAD is contained in live main by ancestry or patch equivalence. `--force`
   permits loss of dirty/in-progress state, not unlanded commits. Explicit `--abandon` requires a verified recovery
   bundle, `sessions/.trash/<ws>-<head>.bundle`, before retiring unlanded work. It is written beside that name and
   renamed into place once verified; a retried removal that finds a bundle verifying for the same range keeps it instead
   of writing another.
2. Stop the supervisor. Ordinary removal quiesces, letting running jobs finish before retirement. `--force` retires
   directly: TERM → grace → KILL across the workspace's descendant process trees (11_shell.md), without waiting for a
   job to complete. Teardown precedes image retirement — live children would otherwise hold the mount busy and keep
   enforcing stale launch-time authority after the grants disappear. The checkout's Nx daemon outlives its supervisor
   (it detaches from the job that started it) and its cwd is the checkout, so it is stopped as `detach` stops it
   (06_cli.md); anything else still holding the volume is left to step 5's grace and force, never a refusal.
3. Remove the host-side state this workspace put in main's repository, before the image goes anywhere: its reverse
   remote if one was registered, and its worktree registration if it is a git-worktree workspace (remove, then prune).
   This is the one piece of teardown that lives where the user can see it — a remote or a registration naming a trashed
   mount is a broken fetch and a broken `git worktree list` in the user's own checkout.
4. Logically retire: atomically rename the image and its detached grants/CA companions to
   `sessions/.trash/<ws>-<incarnation>.dmg`. The canonical image disappears from enumeration and the command returns
   here (typically well under a second; a stubborn process tree delays it by at most the kill grace). Valid checkpoint
   facts left behind during this asynchronous phase neither republish the workspace nor authorize deletion by
   themselves; malformed, foreign, and duplicate facts remain integrity errors.
5. Background (spawned detached): detach the mount (escalating to `-force` after a 10 s monotonic wall-clock grace,
   including disk-lease and command waits, not merely poll sleeps), unlink the trashed image and companions, every
   checkpoint (including pinned checkpoints), every pre-restore undo image and companion/fact, then the empty checkpoint
   and mountpoint directories. Interrupted cleanup is resumed idempotently by `cowshed gc` only from exact, revalidated
   retirement trash metadata; a missing canonical image alone is never cleanup authority.

The in-progress Git gate follows operation state (`MERGE_HEAD`, `CHERRY_PICK_HEAD`, `REVERT_HEAD`, `rebase-merge`, or
`rebase-apply`), not a leftover `.git/AUTO_MERGE` tree: Git can retain that tree after an operation finishes. A stale
tree alone never blocks removal.

The verified bundle's temporary bare repository belongs to the removal attempt, not the next `gc`: cancellation or an
interrupted verification removes it before leaving the operation.

Removal persists its exact options before the authorized retirement mutation. If a process dies after canonical
retirement but before acknowledging the call, startup observes the workspace absent and records completion without
retiring it twice. A prepared request that never crossed the safety fence is discarded, not a deferred deletion; an
authorized mutating request reruns the safety fences and retirement.

A named `rm` can retire an unfinished fork without activating it. The exact pending image is mounted and validated under
its image lock, then receives the same dirty-state, HEAD-stability and landed-work checks as an active workspace. A
refusal preserves the image and original clone intent. Once retirement is authorized, its journal record retains the
original clone source so an interrupted removal can recover.

Removal-scoped startup never finishes the named target's own unpublished clone: it leaves the clone intent for the `rm`
to retire, so whatever stopped the clone — a duplicate port block, a start revision the source no longer holds — cannot
also stop its removal. An unfinished clone intent that left no pending image is retired the same way, with only its slot
binding to release; under the intent lease, so a live creator is refused. Only the named target is retired; its peers
keep their grants. The retired image is validated by its exact trash path, repository, format and incarnation; being
unpublished before retirement does not invalidate that recovery fact.

`cowshed rm main --restore` is the adoption rollback described above and maps to `RemoveOptions.restore`; plain
`cowshed rm main` requires `--force` and a clean `git status` (exit 4 otherwise).

`cowshed rm main --restore --purge [--force] [--abandon]` (`Coordinator::remove_project`) removes the whole project in
one call: every session workspace, including clones a create never published, under the given `--force`/`--abandon`;
then it waits for the background reclamation those removals started, so its collection plans against a settled store;
collects; with `--abandon` deletes the abandon bundles in `sessions/.trash` (then the only copy of those commits); and
restores main. Each step is idempotent, so a refusal leaves the rest to the same command. A collection that still finds
the store changed under it (another process) is refused as retryable — `Conflict` with `Retry::GcPlanStale` — and the
same command finishes the removal; callers never sleep and retry `gc` themselves.

## `cowshed attach` / `cowshed detach`

Explicit attachment lifecycle for long-lived workspaces: `detach` first stops admissions and drains the supervisor and
Linux connector, unlinks the Linux per-incarnation socket, then frees the attachment; the image/dataset and grants
persist. `attach` mounts at the canonical path with canonical flags and, on Linux, creates the per-workspace
socket/netns and starts exactly one connector before admitting execs. Personal macOS workspaces are typically attached
at login by a launchd agent; Linux uses its platform service/controller lifecycle.

## Workspace environment: `.cowshed/env`, optionally sourced by direnv

Every workspace image carries its environment in the in-image private namespace as a plain `source`-able file:
`.cowshed/env` exports `COWSHED_WORKSPACE_TOKEN` (controller-minted, read from `.cowshed/token`), and on macOS
`COWSHED_PORT_BASE` and `COWSHED_PORT_BLOCK_SIZE` from detached metadata's exact `portBlock`. Linux has no port block
and omits both lines. One function derives the file from the image's published token and the workspace's recorded
platform and block, and it is the file's only writer: create, fork and restore publish it as they mint, and every
supervisor start publishes it again, so values are always current — a workspace minted before a variable existed gains
it on its next start. The supervisor start alone also exports the sandbox's `TMPDIR` and `COWSHED_RUNTIME_LINK`, the
workspace's short runtime link (04_sandbox.md): a mint may run at a staging mount, and the link is named after the mount
the workspace is served at. Nothing is derived from cwd, guessed from a slot, or trusted from a marker alone.

Shell hooks are optional repository input. Cowshed never creates or rewrites `.envrc` or `.envrc-local`, and adoption,
fork and restore do not require either file or any ignore rule for it. A repository with no shell configuration uses the
ordinary sandbox environment; a project-owned `.envrc` needs no cowshed-specific loader.

Projects that want interactive direnv shells to receive the published workspace values may add this optional line to
their own hook:

```bash
source_env_if_exists .cowshed/env
```

Agent processes receive the same workspace values at spawn from the supervisor (04_sandbox.md), without sourcing this
file or requiring direnv. No verb exists to print these exports on demand.

**Empty mountpoints are not user-visible state.** A mountpoint directory exists only while its workspace is attached;
detach removes it. Main workspaces are always-mounted: the gateway mounts every main across every adopted project before
reporting healthy (05_gateway.md), and doctor treats an unreachable main as a critical finding. There is no
detached-main state for a user to stumble over.

## Tradeoffs

**Templates rejected.** A template registry (SHA-keyed images, refresh pipelines, promotion, rollback) reproduces CI
inside a workspace manager and demands configuration. Main-as-base gives a fresher starting state for free: main is warm
because it is used. The secrets invariant is what makes this safe; it is enforced, not assumed.

**Linked git worktrees are not the default, and are available on request.** They couple a workspace to main's
`.git/worktrees/<id>`: a sandbox hole into the user's object store, a registration that outlives an interrupted
teardown, a gitdir that is invalid the moment the image is cloned, and a workspace that is unusable whenever main is
detached. A standalone `.git` arrives via CoW at zero marginal cost and the pull-based `cowshed push` replaces
shared-state semantics with an explicit, auditable hand-back — so clone is what `cowshed fork` does. `--git-worktree`
takes the trade deliberately, per workspace, and gives up checkpoint and restore for it (see "Git-worktree workspaces").

**A git credential broker rejected — sandboxed git fetches, never pushes.** A smart-HTTP credential broker on the
gateway would let sandboxed git reach real remotes with push, a git wire protocol to proxy faithfully, and
host-granularity policy that git defeats: Anthropic's sandbox-runtime README warns that allowlisting github.com lets a
process push to any repository (https://github.com/anthropic-experimental/sandbox-runtime), and yolo-cage had to build
an entire git-command classifier to police in-sandbox remote git (https://github.com/borenstein/yolo-cage). Admitting
exactly `git-upload-pack` on an intercepted grant keeps fetch and removes the class: a push is `git-receive-pack`, which
no grant admits; work leaves a workspace as a controller fetch-from-workspace (`cowshed push`); and no credential exists
inside a sandbox.

**hdiutil shadow files rejected.** Shadows share only a frozen base (identical to what clonefile already provides),
measure 2.3× slower on synchronous writes, and pin their base image against deletion. Clones have no runtime dependency
on their source — main's image can be replaced or rewritten (`defrag`) freely.
