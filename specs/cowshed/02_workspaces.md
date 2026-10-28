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
own warm `main`, sessions, grants, gateway identity, and standalone Git object store under its primary `repo_id`.
Commands select the project explicitly with global `--project <git-root>` or derive it from cwd; `detach <ws>` and,
outside any project, `rm <ws>` derive it instead from the one project whose store holds a session of that name. `fork`,
`push`, and `land` never select a machine-global main or cross repository boundaries.

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

The invariant is checked once, where it is cheap: one built-in scanner (`cowshed-core::secrets`) gates `cowshed adopt`.

- **`cowshed adopt` — blocking gate.** Full-tree scan before the image is created (skipping cache roots and `.git`
  objects). Findings refuse the adopt (exit 4) with per-file stderr guidance: migrate the value to the gateway Keychain,
  delete the file, or `cowshed adopt --quarantine`, which moves findings to
  `/private/cowshed/store/<owner>/<repo>/quarantine/` (the primary component-safe `repo_id` path; mode 0600, original
  paths preserved) so dependent tooling fails loudly instead of leaking silently. There is no "adopt anyway" flag —
  main's image is cloned to every future workspace; adopt is the one moment the invariant is cheap to establish.
- **No other verb scans** (the owner's ruling, 2026-10-28). `fork` does not scan each clone, and `push`, autosave and
  `doctor` run no secret gate or audit; none is planned. A secret written into main after adopt is the operator's to
  keep out: credentials belong in the gateway Keychain, never in a checkout.

Detection is convention-tiered, no configuration: filename rules (`.env*`, `*.pem`, `id_*`, `.netrc`, `.npmrc`/`.pypirc`
with auth keys, cloud credential dirs) plus content rules for well-known token shapes (`ghp_…`, `xoxb-…`, `sk-…`,
`AKIA…`, PEM blocks) and `.envrc`/shell exports of `*_TOKEN|*_SECRET|*_KEY` variables. No entropy heuristics — cowshed
prefers zero false positives over exhaustiveness, and says so. Waivers are explicit and controller-owned: per-path
entries in the project's waivers file next to the grant files (never inside the image), each with a required `reason`; a
waiver unblocks adoption while every waived finding stays retained for audit.

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
   name whole, so the name only ever holds a complete, formatted volume. One exact-image in-kernel attach (01a_apfs.md
   §Mounts) attaches it, pinned `fsck_apfs -q` verifies it, then `mount_apfs` mounts its exact owned volume at a staging
   mountpoint (01a_apfs.md §Mounts). That attach is the only one main gets.
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
   with `funmount` on its held, proven root descriptor (01a_apfs.md §Mounts), then activate the sidecar from
   `PendingFence` to `Active` in one atomic rewrite — the publication point. Only then does the checkout path change
   hands, in the way the chosen layout prescribes, and the same pinned attachment mounts there with `mount_apfs`. The
   guards recheck exact image/device ownership after the host lease wait, and the held mount root/source is verified
   before handing the checkout back (01a_apfs.md §Mounts). The normal path has no detach, image rename, second attach,
   or second fsck: an attached image is never renamed, because its identity is the backing path it was attached from.
6. Finish main's first build-state touch, then activate its root shell once through its canonical supervisor when an
   .envrc is present, including projects without Cargo. Require a current activation record (11_shell.md, Inherited
   shells) before adoption completes. Publication has already happened: a failure reports adopted main and the shell
   failure, leaving main in place for repair rather than charging its first fork a cold activation.

### Re-adoption: space reclamation

`cowshed adopt <checkout> --re-adopt` replaces an adopted main through the same staged copy, verification, publication
fence and checkout handoff as adopt. Default adopt remains idempotent; replacement is explicit. Re-adoption is the RAW
guest-space reclaim operation, not an image converter: every input is RAW, and a recorded input in any other format is
refused, naming its format and path.

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
   its built-at tree cleared, and any staged copy an earlier attempt made of it is committed to `.deleting` like every
   other release wherever the replay meets it (05_gateway.md, "Deleting released images"), because nothing can verify it
   any more. A build replacement already published at the build image path under the recorded owner record verbatim no
   longer reads its retained image: losing that image afterwards keeps the published replacement and its record. A lost
   main image refuses as before. Leave old-incarnation seed images unchanged as retained evidence; publish one fresh RAW
   seed from the verified warm replacement for the new incarnation. A resumed copy leaves already-correct hard-link
   topology untouched. It relinks only missing, mismatched or recopied-primary aliases and restores every changed
   alias's ancestor metadata after the subtree is complete. Already-mirrored file payloads are not recopied merely
   because the transaction was interrupted; full content and metadata verification still runs before publication. The
   exclusive claim on the recorded source build image's `<image>.hold` stays held through retention, copy, seed capture
   and publication. A seed of a volume published at that image's own path, warm or empty, captures under that same typed
   claim rather than opening a second exclusive claim on the same resource. A published replacement or kept build
   already detached is seeded before its first mount, not mounted merely to detach and restore it for capture. The claim
   remains held through that mount and the new seed's captured-clone freshness mark. This immutable captured-clone clock
   replaces live-source re-stamping after mount. A replay keeps an existing seed's mark: it never declares old captured
   bytes current by sampling the live image again.
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
   Verify the remounted content, Git state and build identity against the recorded before-state. Return every retained
   path as data (`ReAdoption.retained`, 07_api.md); the CLI renders the explicit later-removal instruction from them,
   and startup recovery, which answers no caller, logs them on stderr. Keep the old backing image and original
   `.pre-cowshed` until the user removes them after verification; repeated invocation resumes the transaction rather
   than replacing its evidence. Both `adopt <checkout> --re-adopt` and `mount main --repo-id <owner/repo>` reach this
   recovery without a live Git checkout, but only from the strict unfinished journal's exact repository/checkout match.
   After startup finishes that intent, retrying adopt retrieves its recorded replacement rather than beginning another
   reclaim or retained generation. A recorded retained source temporarily remounted read-only at its canonical checkout
   remains a recovery input, not an active workspace. Repairing startup recognizes it only when the strict unfinished
   journal's old repository, checkout, workspace and incarnation match its marker, and the kernel joins that exact
   retained image to a read-only mount at that checkout. Recorded main/build inputs restored read-only at their
   canonical paths move back to their recorded copy stages without force. Foreign paths and read-write retained mounts
   refuse.

#### Missing main image

When main's recorded image is missing (no canonical image, sidecar or CA key) and a plain Git checkout sits unmounted at
main's checkout path, `adopt <checkout> --re-adopt` re-adopts main from that checkout. It refuses unless the checkout is
the root of a Git repository one of whose remotes names the project's `repo_id`, and unless main's completed re-adoption
journal for that checkout names the lost incarnation. That journal is the only authority left for main's metadata, image
capacity (unless `--capacity` is given) and recorded build-volume identity. The repository identity and project records
are kept. The checkout is the copy source of the same staged copy, step 4 verification and `PendingFence` publication as
adopt, and it must have no holders, as in step 1: a process with a file, its working directory or its executable inside
the checkout refuses, named by PID and command, and nothing is stopped. At handoff the checkout is swapped aside to
`<checkout>.readopt-<replacement incarnation>`, never `.pre-cowshed`, and kept there as the retained evidence. Main's
build volume is the project's one build record owned by `Linked main`, whatever its ID: a build volume is an image of
its own, so it can outlive main's image, and every land gives main a new one. A surviving one is kept as it is, its
holders refused as in step 1, and seeded for the new incarnation. More than one build record owned by `Linked main`
refuses with integrity. Only when none survives is main's build volume published as an empty RAW volume under the
recorded ID, `Linked main` owner and creation time, with its built-at tree cleared; a build image or record still
present at that ID then refuses: this recovery never publishes over a build volume. Before the Git state is recorded,
the checkout takes main's cowshed excludes and a build link naming main's build volume, so the copy verifies and carries
the link. Old-incarnation seeds are retained and one fresh seed is frozen as in step 3. The journal records the copy
mode, so a replay resumes the same recovery. While nothing is published, the checkout is the user's live tree: a replay
before the handoff proves it unheld again and records its Git state again before it copies, so a commit or a fetch made
after an earlier attempt refused never wedges the recovery.

Re-adoption copies filesystem entries, not allocated-but-free guest blocks. Space returns only after verification and
explicit release of the old image and any snapshots sharing its blocks (01a_apfs.md, "Space reclamation"). Retained
inputs are RAW and attach read-only in the kernel like every image (01a_apfs.md, "Mounts"); an input in any other format
is refused by its format and path, never converted or attached through DiskImages2.

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
verified, and reported as abandoned work against the retained checkout's branch, with nothing to preserve as refs
(`AbandonedWork.preserved` is `nothingToPreserve`). `--abandon` on main without `--restore` is a usage error.

Main's image is the repository that holds every `refs/cowshed/*` ref cowshed wrote: the branches `push` preserves. The
restore retires that image, so before the swap it fetches each of those refs, unforced, into the retained checkout under
the same name, reads each back, and refuses (naming the refs) when one is missing or holds another tip; a `push` staging
ref is transient and stays. The retained checkout is the repository the user keeps, so the refs outlive the project
there, and a later removal of that checkout is what would lose them; the restore says so on stderr.

Unbinding is last. Removing main first records the checkout path in the project store (`checkout-root.json`), because
main's images — the only other record naming it — are retired and then reclaimed. A restore interrupted after retiring
main is finished by the same command from the checkout: the project root lookup matches that record when no main image
exists, which is what reaches a checkout with no remote. The finishing run reports no abandoned work; the bundle the
interrupted run wrote stays where it was reported. Unbinding first releases every build volume the project still has:
with main retired nothing links one, so main's linked volume and its seed go with whatever an earlier collection left
(16_build_volumes.md, "Garbage collection"), and a volume that stays (a job holds it, its release failed) refuses the
unbinding while the project is still bound, naming it and why, for the same command to retry. A release only commits,
and the unbinding is the project's last act, its store tree going with it, so before it plans terminal storage it moves
each released image of its project, from `build/.deleting` and `sessions/.deleting`, into
`<store>/.deleting/<fresh id>.dmg` and nudges the gateway for each one (05_gateway.md, "Deleting released images"). It
frees nothing itself, and a cut in progress moves with its entry, held open by the cleanup. Anything there that is not a
released image stays, and the removal of terminal storage that follows refuses, naming it. Unbinding then removes
terminal storage (`.staging/`, `sessions/`, `tmp/` and `build/`, which by then may hold only empty directories and
zero-length locks and holds; anything else refuses, named, before one entry is removed) and the empty mount trees
(`<owner>/<repo>` and `.build/<owner>/<repo>` under the host mount root). It then unlinks the validated binding and
syncs its held parent directory. Only this durable unbinding authorizes removal of the controller's lifecycle journal,
recovery-export proofs, deletion log and slot bindings; the checkout-root record goes afterward. An unlink failure or
interruption before the sync keeps that authority. A retry finding the binding absent still performs the directory
durability barrier before releasing controller metadata. What the user wrote stays: `policy.json`, `waivers.json` and
`quarantine/` keep the store directory. Once the project is unbound, the identity's project lease
`.retirements/<owner>/<repo>.lease` goes too, and with it an owner directory it leaves empty, but only under the lease
itself: the unbinding's own shared hold must upgrade to exclusive without waiting, and no `<repo>.json` retirement
record may stand beside it. Another holder (the gateway, a command still open on the project) or a record keeps it, said
on stderr, and the unbinding keeps its shared hold. Because every holder rechecks the lease's inode after locking
(`forget`, below), removing it cannot split them. The restore checks terminal storage before it retires main, exempting
what main's retirement and this release remove (main's temp dir, every build volume's image and record, and the deleting
directories of `build/` and `sessions/`, which the unbinding finishes first), so its refusal leaves main in place. The
refusal names what owns the rest first: a trash image, which `gc` releases, then `sessions/` before `tmp/`, so a live
workspace's image is named rather than its temp dir. Binding removal becomes completion only after its parent sync; a
retry finishes controller cleanup before removing the lease, without recreating project directories. A fresh controller
can discover an interrupted unbinding from the exact canonical checkout-root record and a typed, unfinished
`Mutating Retire(main, restore=true)` in the source journal, even for a checkout with no remote. This is read-only
recovery admission, never active inventory or registration. Live bindings retain precedence; ambiguous matching
continuation records refuse rather than choosing one. That unfinished record is the restore's own: no startup replay or
other verb completes it while the project is bound, and a restore is never answered from a completed record, so a retry
after any failed unbinding (a stray file, a release that cannot move) runs the unbinding again rather than reporting
success while the binding stands. A released image the unbinding lists but finds already unlinked by the gateway's
cleanup is released, not an error. The lifecycle journal is released after the other controller records, so failures in
their removal retain the continuation. A death after releasing the journal but before deleting the checkout record can
leave only that unbound bookkeeping record; it holds no recovery authority, and a later adoption's main image outranks
it.

## `cowshed forget <owner/repo> --abandon` — retire from store authority

An orphaned or corrupt registration cannot depend on opening its missing checkout to be retired. `forget` names the
exact repository identity and requires explicit `--abandon`; it does not take `--project`. It does not infer authority
from a fixture-name prefix, an absent process, a missing resource-owner marker, or a temporary root that disappeared.
Ordinary fixture teardown still proves resource retirement through its owning waits. `forget` is a separate, explicit
operator authorization to stop serving a registered project while preserving its evidence.

The positive authority is the durable **`authorization: "explicitAbandon"`** field in
`.retirements/<owner>/<repo>.json`. The record also binds its operation UUID, exact repository identity and SHA-256 of
the original `repository.json` bytes. A valid binding naming another project refuses; a corrupt binding is preserved
byte-for-byte rather than silently replaced. No positive record means no collection.

The monotonic phases are:

1. **`fenced`**: publish authorization before effects. Project actors and startup healers take shared kernel leases
   before binding writes or mounting, and refuse a pending retirement. Existing actors keep their leases; admitted
   blocking build/seed work, cancelled mint cleanup and background reclamation retain their own holds until they finish,
   even if their caller is gone. The project lease is the inode `.retirements/<owner>/<repo>.lease` names: every holder,
   shared or the collector's exclusive, checks after locking (and after a refused attempt) that the path still names the
   inode it locked, by device and inode without following a link, and reopens it when not. So the one removal of it, an
   unbinding's under its exclusive upgrade (above), leaves every holder meeting on the inode at the path. A separate run
   lease excludes concurrent execution and rejects a stale operation generation.
2. **`drained`**: the exact workspace supervisors answer the existing drain protocol, and kernel socket-peer exit
   watches prove they ended. Their socket leases and the existing lost-job-group reconciliation remain held. An unknown
   response, unresolved group, active controller, or held lifecycle intent refuses; a PID lookup cannot substitute for
   those receipts. The collector takes the project's exclusive lease only after the owners release their shared holds.
3. **`detached`**: release each exact backing image with `WhenIdle`, never forcing an unmount or eject. A shared
   no-follow inode hold prevents reattachment between release and archival. Registration drift, changed image identity
   or any remaining attachment refuses and preserves the record.
4. **`archived`**: atomically rename the complete project store tree into `.retired-projects/<operation-id>`, preserving
   its images, dirty Git state inside them, build state, grants, keys and diagnostic records. Preservation and removal
   from the live registration namespace are one rename; neither the missing checkout nor its `.pre-cowshed` tree is
   deleted. Record the completed receipt after syncing the directory changes.

Retry resumes the recorded operation. A crash after rename but before the final phase write is recognized by the exact
archive and registration digest, not by absence alone. Startup discovery excludes positively pending retirements from
ordinary mount healing. After the gateway binds successfully, it resumes only already-authorized records before that
healing; it never writes an abandonment because a fixture vanished, and one refusal cannot prevent unrelated projects
from healing. Completed archive receipts remain, but do not reserve a repository identity against a subsequent
deliberate adoption; a new retirement has a new operation UUID.

This releases mounted devices and removes the live registration; it does **not** claim to free the retained image bytes.
The reported archive is the preservation artifact. `gc` does not discover that reserved namespace as a live project or
silently delete its evidence. The one thing freed there is a released image already waiting in the project's deleting
directories (`build/.deleting` and `sessions/.deleting`): it is no evidence, and the gateway's cleanup frees it where
the rename left it, its startup listing finding it there (05_gateway.md, "Deleting released images"). Normal
`rm main --restore` and its verified Git-containment rules remain unchanged.

The native qualification runs ten complete attach/mount/busy-refusal/holder-close/forget cycles. Every cycle records
`ioclasscount` before and after and proves that the owned image is no longer registered while its archive remains. The
[recorded run](../../packages/cowshed/docs/project-retirement-counts.json) kept `AppleDiskImageDevice` and
`DIDeviceIOUserClient` at 163 in every pair, and `IOMedia` at 176; `AppleAPFSVolume` grew by one per detach, 2185
to 2195. That departed-volume retention is separate from image-device leaks and from retained archive bytes; the run
performed no daemon restart or helper kill and does not claim to fix Disk Arbitration's retained volume references.

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

A fresh mint starts from a clean slate: the workspace's `.git` arrives by CoW carrying every remote its source had,
network URLs included, so mint **strips them all** before configuring its own. That is the "no remote URL ever exists
inside a sandbox" invariant, not a judgement about user intent — nothing in a just-cloned image is the user's yet. The
one remote it keeps is cowshed's own `main` already naming main's canonical mount, which configuration would recreate
unchanged: a fork of a workspace keeps that remote's tracking refs, and a replayed mint keeps what its first pass
recorded.

**A fork carries main's ref.** A workspace resolves `main` with no fetch: `main/main`, `main` and
`git merge-base HEAD main` name the commit the workspace forked from. A clone of main carries main's branches as they
stood at capture, so before mint prunes them (fork mint step 6) it records them as the `main` remote's tracking refs,
and main's checked-out branch as `main/HEAD`, through one fetch from the clone's own repository: no objects move and
nothing outside the workspace is read. A fork of a workspace carries that workspace's `main/*` refs as it last saw them.
Like any tracking ref they are snapshots that `git fetch main` refreshes; `rebase` fetches before it reads them, and
`land` reads its target live.

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

`cowshed rm` drops the reverse registration as part of retirement, before the image is released or preserved, and `land`
drops it at the same point it prunes `refs/cowshed/<ws>/*`. A registration left behind by an interrupted retire is
removed idempotently by `cowshed gc` from the same revalidated retirement metadata that authorizes the rest of the
cleanup — a remote naming a mount path that no longer exists is never by itself authority to delete anything.

## `cowshed fork [<src>] <ws>` — create a workspace

The source defaults to `main`, or names any session. The clone carries the source exactly as captured, including
installed dependencies, build state, tracked edits and untracked files. Every fork runs this one mint path and switches
to its own `cowshed/<ws>` branch from the source's `HEAD`, never the source's branch. Grants start closed with a fresh
CA and platform endpoint identity; neither source grants nor endpoints carry over. No pool or pre-warming. The source
image remains live and mounted throughout capture. A fork never asks about or waits for gate-class work (05_gateway.md,
"Host scheduling").

1. `flock` the source's image lock. Flush its source volume and backing file for freshness without unmounting,
   detaching, sealing, or changing its mount flags. Source shells, editors and watchers continue to use it.
2. Open the source inode without following symlinks and descriptor-clone it to `sessions/<ws>.dmg`. RAW has no outer
   allocation map; the filesystem snapshot is crash-consistent, not an application-state transaction. Create the
   complete closed-baseline sibling sidecar before attach; allocate `portBlock` only on macOS and omit it on Linux. Once
   this clone and the build volume's seed capture beside it are made (16_build_volumes.md, "Fork"), the source's image
   lock is let go: every later step reads only the clone. A git-worktree fork keeps it until it is registered in main.
3. Attach the exact clone once without mounting (`hdiutil attach -kernel -nomount`, flags per 01a_apfs.md §Mounts),
   verify the freshly probed, pinned APFS volume with `fsck_apfs -q`, then use `mount_apfs` to mount that exact volume
   at `<mount-root>/<owner>/<repo>/<ws>`. The mapping is guarded again after the host lease wait; verification precedes
   the first mount, and the held mount root/source is checked before publication. No combined mountpoint attach,
   stale-family selection, fallback or retry is allowed. RAW makes no fixed attach-latency guarantee. Nothing writes the
   clone before it mounts: its first mutation, the read-write mount, makes APFS copy the extent list it shares with the
   source image (01a_apfs.md, "Clone cost follows extents, not size"), seconds per hundred thousand extents, after the
   source's image lock is let go. The fork's process holds the clone's descriptor in that mount the whole time, so a
   process-table census that asks about it waits as long. A census therefore asks nothing of a process a lifecycle lease
   names ("Holders cowshed does not run").
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
   live, and is not walked. A standalone clone then drops the linked-worktree registrations it inherited from main
   (`.git/worktrees`) before any link is judged: they are main's, never the clone's, and the hook links a stale one
   carries name a worktree that may be long gone. Then re-resolve inherited escaping symlinks. A relative symlink whose
   target climbs above the tree root (the entry `bun install` writes for a `link:` dependency, say) was computed against
   main's depth and lands somewhere else at the workspace's mount depth, so it is rewritten to the absolute path it
   named in main; one whose target does not exist in main either is refused by name rather than repointed at a guess. An
   absolute symlink whose target lies under the source checkout's root (a hook installer's `.git/hooks` entry, a package
   linker's link to a directory the checkout owns) names the source checkout's own files, which the workspace's sandbox
   cannot read and whose edits belong to another checkout; it is rewritten to the same path under the clone's root,
   spelled relative to the link, so it names the clone's own copy at every later depth. In-tree links and absolute links
   outside the source root are left alone. Before any private job runs, the mint's identity adds exactly the canonical
   sibling targets of the source's developer package links to its read grants (16_build_volumes.md, "Hash inputs"); it
   never adds an ancestor dependency store or the source's main root, and the source's owned-link marker is migrated
   first. Finding them reads every directory of the fresh volume, concurrently, and is the largest setup cost after the
   clone's first write. Then, inside the mount, under the workspace's closed sandbox: configure the `main` remote and,
   in a clone of main, record the branches it carried as that remote's (see "The `main` remote"), then
   `git switch -c cowshed/<name>` from the checked-out state, then delete every other local branch. One branch per
   workspace, as in a git worktree: the workspace's own branch is the only one it rebases on main and lands, and main's
   branches stay main's, reached through the `main` remote. Nothing in the clone is the workspace's work yet, so every
   carried branch is a copy of the source's as captured, whether or not the source has since moved or dropped it; a copy
   left in the workspace is a branch work can land on unseen and one `rm` must judge against main. A fork from a session
   keeps none of the source's branches either, its `cowshed/<src>` included. The seed contributes only build state
   (16_build_volumes.md); every Git byte comes from the source's own image. The `.git` directory arrived complete via
   CoW — the workspace is a standalone repository with **no linked-worktree registration and no back-references** into
   the host checkout, unless it was created with `--git-worktree` (see "Git-worktree workspaces").
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
failed. A verb whose fresh standalone clone's initializer refused does the same once it has reclaimed that clone (see
"Publication and crash recovery"). Startup first reconciles a pending destination against canonical inventory: an
already-published incarnation becomes the recorded result; an absent destination reruns the operation if its intent is
`Mutating`, or if a `PendingFence` image or a slot binding carries its name. Binaries before the `Mutating` mark
journaled every create and fork `Prepared`, so those durable facts, not the phase, decide a `Prepared` intent. One with
neither mutated nothing; startup discards it and names it on stderr instead of rerunning it. Reissuing the same call
returns the recorded incarnation instead of creating a second workspace or reporting a false conflict.

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
Startup recovery acts only on unfinished intents whose lease it can take, one at a time, and never holds a lease while
it waits for an image lock (01_storage.md, "Lock order"): a lock it finds held, `main.dmg.lock` under a fork's clone
say, makes it let go of the lease and wait for the lock alone, so a verb re-run on that name meanwhile takes the lease
and finishes the intent itself. An intent whose lease another process holds is that process's running operation, not
crash residue, and is never run a second time beside it. `cowshed doctor` runs no startup recovery at all: it reports
every unfinished intent and leaves it journaled.

A `PendingFence` clone that no unfinished intent names, and that no process is creating (its intent lease and its
image's lifecycle lock are both free), is abandoned: nothing will finish it, and because it was never published nothing
ever ran in it. `gc` and `doctor --repair` retire it as the create or fork its own metadata records, under the ordinary
pending-clone retirement checks, and `rm <name>` accepts it the same way. A retirement those checks refuse leaves the
clone in place and names the refusal. `gc` never deletes a `PendingFence` image directly.

| Kill window                                                      | Durable state                                                  | Recovery action and guard                                                                                                                                                                                                                                                                                                     |
| ---------------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Before the `Mutating` mark                                       | `Prepared` intent only                                         | Nothing durable exists. A failing verb puts back the record its intent superseded; startup discards a leftover one and names it on stderr.                                                                                                                                                                                    |
| After the mark, before pending metadata                          | `Mutating` intent, plus the slot binding with `--slot`         | Re-run create/fork normally. A `Prepared` intent whose slot is bound (an older binary, or a kill between binding and marking) is re-run the same way.                                                                                                                                                                         |
| After sidecar, before complete clone                             | Sidecar only, or a failed clone                                | A clone failure reclaims both artifacts; startup recovery removes a sidecar-only record. No partial payload is admitted as resumable.                                                                                                                                                                                         |
| After clone, before attach                                       | C + `PendingFence`                                             | Reuse the metadata incarnation and original operation identity; never clone over C.                                                                                                                                                                                                                                           |
| After attach, before mount                                       | C + `PendingFence` + unmounted attachment                      | An exact inventory match is detached and settled, then the ordinary verified attach/fsck path repeats before mounting.                                                                                                                                                                                                        |
| After mount, during image-local preparation                      | C + `PendingFence` + canonical kernel mount                    | Exact source-device and canonical mount flags reuse the existing attachment; ambiguous or foreign mounts fail closed. Rename and credential publication are idempotent.                                                                                                                                                       |
| During the initializer                                           | The same pending mounted C, possibly with external Git effects | Cancellation preserves C and the mount, and so does an initializer error on a resume or a git-worktree clone. The retry receives explicit `stage.resuming` authority; the branch and worktree state machines continue only their exact cowshed-owned state. An initializer error on any other fresh clone reclaims C (below). |
| After the marker write                                           | The same pending mounted C                                     | An already-current marker is validated rather than rewritten, so lineage never duplicates itself.                                                                                                                                                                                                                             |
| After the callback, before or during activation                  | Pending or `Active` sidecar (atomic rewrite)                   | Pending reruns the callback and validation; `Active` is listed as current. No intermediate publication state exists.                                                                                                                                                                                                          |
| After activation, before downstream effects or intent completion | `Active` C + pending lifecycle intent                          | Startup recovery reruns optional main registration and append-safe commitments, then records exact completion. Slot binding is retained across errors, and rebinding the exact owner is a no-op.                                                                                                                              |
| After completion                                                 | `Active` C + completed intent                                  | Reissuing returns the recorded incarnation; supervisor startup is start-or-reuse.                                                                                                                                                                                                                                             |

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

**A refused fresh clone is reclaimed.** Rollback's hazards are a crash's and an external effect's. A fresh clone whose
initializer returns an error has neither when nothing the initializer does reaches past the clone: a standalone clone's
branch, remotes and links live in its own repository, and the build volume and seed it forks belong to its incarnation.
Its verb is still running, nothing was published, and the source still holds every byte the clone copied. So the verb
detaches and reclaims C, releases the slot it bound, withdraws its intent (putting back the record it superseded), and
collects the build volumes the withdrawn intent kept from collection. A refusal leaves the store as the verb found it,
and the next attempt clones the source as it is then. Kept, every retry resumed C, the copy of the state that refused —
a stale worktree registration main had since dropped refused one fork until the clone was removed by hand. A
git-worktree clone's initializer registers it in main's repository, and a resume may carry an earlier attempt's effects,
so both keep C as the table says.

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

`cowshed rm` drops the registration as part of retirement, in the same host-side-state-first step that drops a reverse
remote: remove the worktree registration from main's repository, then retire the image. Unregistering is the removal of
that worktree's administrative directory and nothing else, because both commands that look more obvious are wrong here.
`git worktree remove` deletes the working tree at the registered path — the workspace's own files, which retirement
retires as one payload and may already have released — and refuses on dirty state that `rm --force` has already
accepted. `git worktree prune` unregisters _every_ worktree whose path is missing, and a merely detached cowshed
workspace looks exactly like that. A registration orphaned by an interrupted retire is removed idempotently by
`cowshed gc` from revalidated retirement metadata, before the image that carries that metadata is deleted — never from
the mere observation that a registered worktree's path is not mounted.

The mode is recorded store-side, in the workspace's detached sidecar, for the same reason: every decision it drives —
requiring main mounted, pruning the registration at retirement — has to be made while the workspace is detached and its
mount can say nothing. A standalone workspace's sidecar omits the field.

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

### Writer handoff

Before write responsibility passes from one writer to another, the current writer stops the workspace's jobs and
quiesces its writes. The next writer then receives either a fresh capability for the same attached, writable workspace,
or a live copy-on-write fork of that workspace (`cowshed fork`). A fork receives its own identity and closed-baseline
grants as described above and starts from the workspace's state at the moment it is cloned, committed and uncommitted
files alike. The source workspace is not changed by the fork and remains recoverable until the fork is accepted or
independently preserved. A failed writer can therefore be replaced on the same workspace, or its attempt abandoned in
favor of an unaffected fork, without conflating handoff with publication or retirement.

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
retire. Autosave runs no secret scan ("Secrets enforcement": adopt is the only gate). Uncommitted work between autosaves
is the only loss window — images are excluded from backup by design (01a_apfs.md).

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

### `cowshed rebase <ws> [--onto <branch-or-ref> | --into <lane>] [--plan] [--drop <commit>… [--drop-unheld]] | --continue | --abort`

Brings a workspace's branch up to the selected host revision. The default is `main`; `--onto` accepts a validated local
branch or fully qualified ref in the main workspace repository. An object ID may be used as an immutable replay base,
but `land` still requires a named branch as its fast-forward target. `--into <lane>` selects what the workspace lands
into instead — the branch the lane base `<lane>` has checked out, fetched from the lane base's mount into the workspace
as `refs/cowshed/targets/<lane>/<branch>` (see "Lanes" below); any workspace can be that lane base, so this is how a
shed's work moves onto a batch's branch. `--onto` and `--into` are two destinations for one rebase and are refused
together (exit 2).

- Default mode: fetch the selected host revision inside the workspace, then replay the branch onto that exact object ID
  by its **plan**. The whole step runs **inside the workspace sandbox** — the fetch from the main workspace mount is a
  read (covered by the baseline), and every write lands in the workspace's own repository. The plan takes each commit of
  `<destination>..<branch>` a rebase replays, merges excepted (a rebase does not replay a merge), oldest first. A commit
  whose own change, from its parent, the destination already holds by the two proofs `rm` step 1 applies, asked of that
  commit alone, is **held**: by patch identity, naming its twin in the destination, or by content, naming the
  destination commit that changed its files last. A held commit is left out of the replay, wherever it sits: a unit
  stacked on another unit's commits keeps them after its lane folded them as rewritten twins, and replaying those over
  the lane's own version, once the lane edited past it, would conflict, while an amended copy of a folded commit is not
  held and replays, so nothing is dropped that the destination lacks. Every other commit **replays**; the plan replays
  each in memory after the ones before it (`git merge-tree --merge-base=<its parent>`, into a scratch object store) and
  marks one whose merge conflicts **conflict-expected**, naming its paths. The replay is one interactive rebase whose
  todo is the plan's replays and nothing else (`noop` when none replays), run with `--no-autostash` whatever the
  repository's config says, because an autostash re-apply that conflicts leaves unmerged paths and a parked stash yet
  exits 0. A dirty tree is refused before anything moves (exit 4, commit or discard first). A commit whose replay comes
  out empty, its change already held, is dropped and reported held. Every rebase reports, on stderr,
  `replayed N, dropped K held (<commits>), dropped U unheld (<commits>)`, and in `--json` each commit with what became
  of it (`RebaseReport.rebased.commits`).
- `--plan` answers that plan (`RebaseReport.planned`) and moves nothing: no branch, worktree, rebase state or recorded
  plan, only the fetch of the destination every rebase makes. Read it first.
- A replay that conflicts **stops in progress** and exits 4, naming the commit and its conflicted paths
  (`FenceRefusal::ReplayStopped`). Resolve the paths in the workspace, then `cowshed rebase <ws> --continue`: it refuses
  a path that still carries a conflict marker, stages the resolved ones and replays the rest, stopping again at the next
  conflict. `cowshed rebase <ws> --abort` puts the branch back at the head it started from. Both carry on only a rebase
  this verb started (`<git dir>/cowshed-rebase.json` names it); a rebase in progress that cowshed did not start refuses
  the verb, and cowshed never aborts or deletes it. Nobody finishes a stopped rebase with `git rebase` by hand: that is
  how a commit got lost.
- `--drop <commit>…` leaves those commits out of the replay. A dropped commit the destination holds is a held drop
  anyway; one it does not hold is refused (`FenceRefusal::DropUnheld`, exit 4) unless `--drop-unheld` names the loss,
  and is then reported dropped unheld. A `--drop` naming a commit neither the replay nor the recorded plan carries is a
  usage error (exit 2).
- **The recorded plan.** Every rebase records, in the workspace's own per-worktree refs
  (`refs/worktree/cowshed-plan/{base,head}`), the head it starts from and where that head left its destination, and,
  once it finishes, the head it left and the destination. An ungated land's replay records the same. Before a rebase, an
  ungated land and every retirement (`rm`, a land's retire of its unit and of the workspaces it folded), cowshed asks
  which commits of the recorded `base..head` the branch has since lost: neither reachable from it, nor carried by it
  rewritten, nor held, by patch identity or content, by it or by the destination. Such a commit refuses the operation
  (`FenceRefusal::RecordedWorkLost`, exit 4) naming it. A commit of the branch's own, reachable from neither the
  recorded head nor the destination, that has a recorded commit's author, author date and message carries it rewritten,
  one recorded commit each: a replay leaves a commit so, whatever the resolution of its conflict made of its change, and
  `--continue` needs no `--drop` for it. A lost commit is work only that workspace still knew, which a by-hand rebase,
  reset or `--skip` dropped. Restore it (`git cherry-pick`), or name the loss with
  `cowshed rebase <ws> --drop <commit> --drop-unheld`, which records a new plan; `rm --abandon` loses it too and says so
  on stderr. A workspace with no recorded plan has nothing to compare.
- `--fresh`: divergence-shedding rebase. Create a new clone from the selected current host revision, replay the branch
  onto it (`git format-patch`/`am`, or cherry-pick range), then **transplant identity** — the new clone inherits the
  workspace's name, canonical mount path, token, and grant-file binding; the old clone is destroyed (11_shell.md
  teardown ordering). This drops accumulated substrate divergence (ZFS origin-snapshot pin, APFS image growth —
  01_storage.md, 09_substrates.md) and re-warms in-image caches to the selected base. A replay conflict leaves the
  **original** workspace untouched and discards the half-built fresh clone (exit 4). Refuses (exit 4) when the workspace
  has uncommitted changes — replay carries commits only; commit or discard first. `--fresh` is what a coordinator runs
  against a long-lived workspace that `cowshed du` shows has drifted far from its integration base.

### `cowshed land <ws> [--into <lane> [--ungated]] [--target <branch>] [--check <cmd>]`

The full born-from-host-return-to-host close-out, as one primitive. It lands into main, or with `--into <lane>` into
that lane base (see "Lanes" below); "the target" below is whichever one it is. The target branch defaults to the branch
the target's checkout has checked out (`main` for main); a detached checkout with no `--target` is refused with exit 4
before anything runs. How the branch moves depends only on checkout state, read again under the repository lock just
before the update, because the check can run for minutes while the checkout is switched (step 3).

A gated land does not rebase. It lands the head the caller validated and named (`--expected-source-head`), and an
implicit rebase would land a head nobody named whenever main had moved. A workspace whose base main has moved past fails
the fast-forward with exit 4 and a hint naming `cowshed rebase <ws>`; rebasing and re-running the gates is the caller's
step. An ungated land validates no head, so it rebases on its own (`--ungated`, below).

1. **Refuse a dirty tree** (exit 4), read exactly as `rm` reads one (a tracked change, or an untracked file that is not
   hidden, not ignored by main's current rules, and not held by main's checkout with the same content). The check runs
   in the working tree but only the commit lands, so uncommitted work would be validated without landing, and would then
   refuse the retire after main had already moved.
2. **Validate**: run the check _inside the sandbox_ — `--check <cmd>` if given, else no validation with one honest
   `cowshed:` stderr line saying so. Non-zero check → exit 4, workspace intact, output captured as a job (11_shell.md)
   for diagnosis. The check is an ordinary sandboxed exec, admitted at gate class as the land's own work (05_gateway.md,
   "A land is a gate job"), and gets exactly the environment one gets (04_sandbox.md): the workspace's optional
   activated shell over cowshed's own job contract — the bootstrap `PATH` of the `direnv`/`devenv`/`nix` packages and
   system directories, plus a private `HOME`. A verify command that needs more needs it wired into the workspace, not
   leaked from the caller's shell; the check runs where the work was done, and a check that only passes in the
   coordinator's environment has not validated the workspace.

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

   **The repository lock is the target's ref lock, and it fences the head alone.** Writers that move the target branch
   outside cowshed — an operator's Markdown commit straight into main, say — order themselves by a lock file of their
   own, and the land joins them when its caller names that file with `--ref-lock <file>`, or the land-turn queue they
   take instead with `--ref-turn <queue>` (05_gateway.md "A ref turn"), which it takes for exactly the span below, its
   fence's git binding itself to the turn as it starts and stopped should the turn be lost while it runs (05_gateway.md
   "Land turns"). With `--ref-lock`, it takes the file as a held lock ("Held locks" below), the exclusive `flock(2)`
   lockf(1) also takes, re-reads the target head, revalidates the expected target head against it, fetches and
   fast-forwards, and lets go. The fetch and the fast-forward's git are handed the lock's descriptor, so the kernel
   keeps the lock until that git ends: a land killed mid-fast-forward never leaves git moving the ref outside it. Git's
   automatic gc and maintenance are off for them (`gc.auto=0`, `maintenance.auto=false`; hooks and the fsmonitor are off
   for every controller git), so neither detaches holding the lock, and no other process the land starts inherits it. A
   program the target repository's own configuration selects (a filter or merge driver) that outlives git holds the lock
   like any held command's descendant ("Held locks" below): until it closes it or exits, never unlocked around. Nothing
   else runs under it: the check (step 2) before it, and the build-volume adoption and retirement after it (steps 4 and
   5), run unlocked, so a writer queued on the lock waits for this fence, never for a land's minutes of other work. A
   land that finds the lock held waits for it without bound — its holder is a writer of the same branch, which the land
   follows rather than overtakes — and says on stderr every 10 s how long it has waited and who held the file when the
   wait began, as the kernel shows it (`lock-holders`): one look at the holders per wait, never one per report. Its
   `delivery` span is the hold. Measured 2026-10-20 with the whole land under such a lock: an operator's land held its
   ref lock for 82 to 85 s per land, about 56 to 63 s of it build-volume adoption, and one land held it 45 minutes, so
   every Markdown commit queued behind work the lock does not protect. Without `--ref-lock` or `--ref-turn`, git's own
   ref and index locks are the only fence against such writers, and the head is still re-read and revalidated just
   before the fast-forward.

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

4. **Freeze the seed, adopt the build volume and restore checked outputs**: every gated land refreshes the target's seed
   by sealing and cloning the landing workspace's warm build volume before the target adopts it by renaming its
   `.cowshed/build` link. It restores only the attributed Nx task graphs of the checks already completed in the landing
   workspace, from the adopted local cache. A miss or uncacheable task is reported without executing it; check commands
   never run again in the target. 16_build_volumes.md, Land step 7, owns restoration, writer-quiescence skips and
   collection; every seeded/unseeded and adopted/skipped outcome is durable (13_telemetry.md).

   Every land that advances its target then refreshes that target's shell once through its own canonical supervisor,
   gated or ungated, and requires its current activation record before retirement (11_shell.md, Inherited shells). This
   runs no check and copies no export from the landing workspace. A refresh failure names the head and target already
   landed, preserves the unit and reports the activation error; it never claims the fast-forward was refused.

5. **Retire**: only after the target branch and, when checked out, its visible working state resolve to the validated
   source head, destroy the workspace (supervisor tree first — 11_shell.md) and prune its `refs/cowshed/<ws>/*`
   preservation refs on the host, except a `push` ref that keeps a tip the target branch does not hold: `rm` step 1
   counts it as the keeper, so it is then the only copy. The retire gate is `rm`'s, measured against the branch the
   workspace just landed on, in the target's repository: a lane unit whose commits its lane base holds retires, though
   main does not hold them yet. Of the ways `rm` step 1 lets a local branch pass, the branch of the same name (another
   branch's alone) is read in the target's repository, but the `push` ref `refs/cowshed/<ws>/heads/<branch>`, HEAD's
   included, is read in main's repository, where `push` writes it whichever target the unit lands into. A retire refused
   at this point (the check left work behind, say) keeps the workspace and exits with the refusal, but its message
   starts with the landed head and target: the target has moved, so the next step is `cowshed rm`, not another land.

   **Folds.** A land whose unit retired then retires every session workspace it folded: one whose own work the landed
   range carried into the target. Trains fold units by cherry-pick, resolving conflicts and adding review fixes, so no
   folded unit is an ancestor of what lands and none retires by a land of its own. The fold list is the land's own: the
   paths the range from the target's previous head to the landed head changes, and every published, mounted session
   other than the unit, the target workspace and main whose own changes since its merge base with that previous head
   touch one of them. Of those, a workspace is folded when the landed tip holds it by `rm`'s measurement (`rm` step 1,
   against the branch the unit landed on, every local branch included, and its HEAD held by the tip itself: a HEAD only
   a `push` ref keeps is work the tip lacks, so it is no fold); the others are not the land's, and are neither touched
   nor named. A folded workspace is retired as `rm` retires it, measured again under its own land lease, with two
   differences. The age floor does not apply: its work is already in the target. And its owner is spared, never
   quiesced: a supervisor running a job or holding an open shell session refuses the stop rather than waiting for it,
   and a process cowshed does not run that holds its mount refuses as it does for `rm` ("Holders cowshed does not run").
   A folded workspace whose tree is dirty (a tracked change, or an untracked file no ignore rule covers) is kept. Each
   folded workspace is reported in `LandedReport.folded` with its head and its outcome, `retired`, or `kept` with why
   (`dirty` with its file count, or `refused` with the refusal's code and message), and said on stderr. A kept fold
   never fails the land, and nothing about it is stored: it is the next `rm`'s, or the next land's that touches it.
   Retired folds release their build volumes in one collection after the last of them, which runs in the background once
   the land answers (16_build_volumes.md, Land 6.5). A land with `--no-retire` folds nothing.

`--no-retire` keeps the workspace after a successful land (for a coordinator that wants to reuse it via
`rebase --fresh`). `--push-only` performs steps 1–2 and installs the validated head in the workspace's durable
`refs/cowshed/<ws>/heads/<branch>` preservation ref without advancing the target branch, for review-gated flows. It
returns `LandReport::Delivered(PushReport)` before build-volume adoption, land commitments, retirement or collection,
even with the default `retire: true`. An ordinary land returns `LandReport::Landed(LandedReport)`; its existing landed
fields describe only an actual target advance. A preflight returns `LandReport::Preflighted(LandPreflight)` (below). The
canonical JSON report uses the flat `kind: delivered | landed | preflighted` tag.

`--preflight` runs the fences land runs before its check and stops, so a caller learns that land would refuse before it
spends a gate on it. It is a flag of `land`, not a verb of its own, because its whole contract is "what this land would
refuse before its check": it takes the land's own arguments, `--into` included, and runs the land's own functions in the
land's order, so it parses, resolves and refuses exactly as the land does, never by a second copy of a rule. The fences
are each a read: the plan (`--ungated` into main, or with `--check` or `--push-only`, exit 2); the binding gate's reads
(a retired project, an identity move); the workspace and `--expected-workspace-incarnation`; the target (another
workspace than the unit, at the incarnation `--into` resolved); the source head and `--expected-source-head`; step 1's
dirty tree; the target branch (a detached target checkout without `--target`), checked out in the target's checkout; and
the target head and `--expected-target-head`. A refusal is the land's own, word for word: its code, message, hint and
`FenceRefusal`. A pass exits 0 with `{kind: preflighted, workspaceIncarnation, sourceHead, targetBranch, targetHead}`,
the source workspace incarnation, source head and target head that the following land can pin with
`--expected-workspace-incarnation`, `--expected-source-head` and `--expected-target-head`. A missing target head is
reported as `{missing: true}` and passed as `missing`. Reuse `targetBranch` as `--target` and keep the original `--into`
and other land options. These expectations refuse changes to those three values; they do not pin the target workspace
incarnation across independent calls. A preflight mutates nothing: it takes no land lease, so it runs while a land or an
`rm` of the workspace holds one, and no ref lock; the CLI opens the project as an inspection, finishing no interrupted
work. It waits for no install, checks no daemon build and never re-execs the installed cowshed; it heals no binding,
waits for no build, admits no supervisor, synchronizes no gateway and runs no check. Reading a detached lane base would
mean attaching it, so that target answers `environment-missing` (exit 5), never a pass or a land's refusal. What land
decides from its lease on is outside the preflight: a lease another land or `rm` holds, a workspace with no checked-out
branch (read after the check), the check itself, an ungated land's rebase, and the target head re-read under the ref
lock.

`--ungated`, with `--into <lane>` only, folds a unit without a check or build-volume adoption (see Lanes): steps 1, 3
and 5 run, along with step 4's target-shell refresh alone. Before step 3 it rebases the unit onto the lane base's target
branch, so a fold is one command. A unit whose head already holds that branch's tip is based on it and lands as it is:
nothing is fetched and nothing is replayed. Otherwise the land fetches the branch into the unit and replays the unit
onto it as `rebase --into <lane>` does, outside the ref lock; a conflict rolls the replay back and refuses with exit 4
as the rebase does, naming the conflicted paths, with the unit as it was and the lane base unmoved.
`--expected-source-head` names the unit's head before that rebase, and the report's `landedHead` is the head after it; a
rebase that replayed says so on stderr. A lane base that moves between the rebase and the fast-forward fails the
fast-forward as any land's does, and its hint is to run the ungated land again, which rebases again. It runs no check,
and after the fast-forward it freezes no seed and adopts no build volume; it still refreshes the target shell once
before retirement. The lane base keeps its build volume and seed. The unit's volume is still the unit's, so the retire
releases it with the unit, closing its Nx state as `cowshed rm` does. The report is `landed` with
`buildVolume: {seed: {kind: "unchanged"}, adoption: {kind: "skipped", reason: {kind: "ungated"}}}`, and its durable
`LandOutcome` records that skip (13_telemetry.md). A land into main (no `--into`, or `--into main`) refuses `--ungated`
with exit 2 naming main, since only a land into main gates; `--check` and `--push-only` are refused with it (exit 2).
With `--no-retire` an ungated land keeps the unit and collects nothing, since it released no volume.

A target fast-forwarded through plain Git instead is an **external land**: it forfeits output restoration and adoption.
The next fork (including `--ref` naming the target's current tree) automatically captures a live workspace at exactly
that tree as the target's seed, under writer exclusion; no manual seed-refresh or warm command exists. `doctor` reports
`seed-tree-behind` and `target-moved-outside-land` when the recorded volumes do not match the tree.

**A land and an rm of one workspace exclude each other.** From the fast-forward on, a landing workspace's commits are in
the target, so `rm`'s containment gate passes while its land still adopts the build volume and retires it, and the age
floor does not help a workspace older than ten minutes. Measured 2026-10-21: an agent's watcher saw the landed head on
main and ran `cowshed rm` on the workspace while its land was releasing the target's previous build volume; the land,
reading its workspace back afterwards, answered `not-found` and its wrapper skipped the push of a main the land had
moved. So a land, from just after it validates the binding to its end (its own retire included), and an `rm`, for its
whole run, each hold `sessions/<ws>.land.lock` exclusively: a `flock(2)` on a descriptor the process owns, so the kernel
drops it when that process ends however it ends, and a killed land never keeps its workspace from being removed. The
file records the holder (`land`, `remove`, or `make-room` while making room for an attach detaches an idle workspace,
"Making room for an attach"; its pid, since when, its command line); the operation refused names it —
`cannot remove workspace <ws>: it is being landed by pid …` — and is never queued behind it. A land reads its
workspace's incarnation once, at its start, and never reads the workspace back after the fast-forward. A land whose own
retire finds the workspace already gone (removed by a cowshed that predates the lease, or by hand) reports `landed` and
says on stderr that there was nothing left to retire. Who removed it is the deletion log's `retiredBy`.

### A declared land: `cowshed land <ws> [--no-push] [--no-retire]`

A repository whose main checkout's `.cowshed.toml` declares `[land]` (06_cli.md "Configuration") lands into main by one
verb: `cowshed land <ws>` is the whole route from a committed workspace to a pushed main, and no host script
orchestrates it. The route is cowshed's and the gate is the repository's: `[land] gate` names the command, the
repository's script stays the owner of what a gate checks, and cowshed owns when it runs and everything around it. Only
main's copy of the declaration is read, as for `[sandbox]`: a workspace's copy is the agent's to edit, and a gate the
landing workspace could name would gate nothing. A land into a lane base (`--into <lane>`) is the primitive above,
unchanged.

1. **Refuse early.** The land runs `land --preflight`'s fences first, so a land that would refuse after its gate (a
   dirty tree, a retired binding) refuses before it asks for a turn.
2. **The land turn.** It takes its repository's turn, the queue `[land] turn` (05_gateway.md "Land turns"), in this
   process, and holds it through step 5 and its retire: its wait reports, ticket and the gateway's log lines are
   `land-turn`'s, with `<ws>` as the ticket's workspace. The gate (step 4) binds itself to the turn as `land-turn`'s
   command does, from its start, so the gateway keeps the turn until the gate has ended too, however this process ends:
   a land killed alone leaves its gate holding the turn, never a second land beside it.
3. **Rebase.** It rebases `<ws>` onto main's head as `cowshed rebase <ws> --expected-onto-head <head>` does, and again
   onto the newer head when main moved before the rebase began (`OntoMoved`). A conflict stops the land with the
   rebase's own refusal and the workspace as it was: the land aborts the replay the rebase stopped (`--abort`), and the
   conflict is resolved through `cowshed rebase <ws>` and `--continue` before landing again. A turn lost while the
   rebase runs stops the land once the rebase has ended, and one found lost before the fast-forward (step 5) stops it
   before main moves.
4. **The declared gate.** It runs `[land] gate`, `{workspace}` replaced by `<ws>`, on the host in main's pure
   environment (below), its stdout and stderr on the land's stderr. A nonzero exit stops the land (exit 4, naming the
   gate and how it ended) and keeps the workspace. The gate checks the tree; it moves neither main nor the workspace,
   and a workspace head that moved during the gate is step 5's `SourceMoved`.
5. **Fast-forward under the ref turn.** It lands exactly the gated head onto exactly the head it rebased onto (the
   primitive above with `--expected-source-head` and `--expected-target-head` pinned), and its ref fence is the
   repository's **ref turn**, the queue `<turn>.ref` a doc commit holds ("`cowshed doc-commit`" below), taken only from
   the head re-read through the fast-forward. The primitive's retire is the route's: the workspace is retired as
   `cowshed land` retires the unit it landed, however young, with whatever that retire takes with it (`--no-retire`
   keeps it, and the land collects build volumes instead). A main that moved since the rebase (`TargetMoved`: a doc
   commit landed while the gate ran) sends the land back to step 3: it rebases onto the new head and runs the gate
   again, whose Nx cache replays every task the commits main gained do not reach; stderr counts the re-gates. A refusal
   no fence made, with main holding the gated head and that head not the one it rebased onto, came after the
   fast-forward, from the retire: main has moved, so the land goes on to step 6 and answers that refusal last. Any other
   refusal stops the land. Lock order is always the land turn, then the ref turn; nothing holds a ref turn and then asks
   for a land turn, so the two never deadlock.
6. **After the turn.** The turn ends once the fast-forward's retire has. Then, outside it: the landed head is recorded
   in `lands.log` in the turn's records directory (`{atUnixMs, event, head, before?, detail?}`, `event` one of `landed`,
   `unpushed`, `pushed`, `push-failed`, `after-exited`; appended, never rewritten), and the land submits `__land-after`,
   run as the store's installed cowshed (05_gateway.md "The installed cowshed", the gateway's own program), to main's
   workspace supervisor as a background job at the host boundary, with the declared commands' environment, and returns
   without waiting, naming the job. That job pushes the landed branch to `[land] push` and then runs each `[land] after`
   command in order, `{before}` and `{head}` replaced by main's head before and after the land. Its membership, CPU and
   end are a job's like any other (its cgroup on Linux, its process tree on Darwin). The push is keyed on main having
   moved (a land of a workspace main already held pushes nothing), never on the retire: a land whose retire was refused
   records `landed` and submits the job before it reports that refusal, so no landed main is left unpushed. A job that
   is not admitted is recorded as `push-failed` (`after-exited` when nothing was to be pushed) with why, and said. Each
   outcome is one more `lands.log` line, and each command's output is kept in `lands/<head>.*.log` beside it, as it
   comes. The push (`git push --progress`) and each after command have a silence bound of 180 s, not a total, over their
   output and the CPU of what they run: on Linux each runs in a cgroup leaf of its own inside the job's cgroup, retired
   once it ends, and on Darwin in a process group of its own. One that shows neither for 180 s is sent `SIGTERM` and its
   outcome is `SILENT` (05_gateway.md "Deadlines"). The land never waits for the job: a remote is never needed to land,
   and a slow push or install holds up neither this land nor the next. `--no-push` records `unpushed` and pushes
   nothing; the after commands still run. The report is the primitive's `landed` report.

**The declared commands' environment.** Every command a declared land or a doc commit runs (gate, edit, check, commit,
push, after) runs with nothing of the caller's shell. The environment is cleared; `HOME`, `USER`, `LOGNAME`, `TERM`,
`LANG` and `SSH_AUTH_SOCK` are passed on when the caller has them; `PATH` is the host bootstrap path (the installed
cowshed's directory, the host's Nix profile and program directories, the system directories); the command runs in main's
checkout as `direnv exec <main> <argv…>` when main has an `.envrc`, so it sees exactly main's devenv and never the
caller's, and as `<argv…>` when it has none. It gets the held-command policies ("Held locks": `NX_DAEMON_START=never`,
git's automatic gc, maintenance and fsmonitor off). The caller's environment is only identity: a gate that passes in one
agent's shell and fails in another's has not gated anything.

With `[land]` declared, a land into main refuses the primitive's caller-side options with exit 2 naming the route:
`--check` (the declared gate is the check), `--target`, `--expected-workspace-incarnation`, `--expected-source-head`,
`--expected-target-head` (the route pins its own heads), `--ref-lock` and `--ref-turn` (the route takes the declared ref
turn), `--push-only` and `--ungated`. `--preflight` runs the fences alone, as for any land. `--no-push` is refused where
`[land]` is not declared, since only the route pushes.

### `cowshed doc-commit (-m <msg> | -F <file>) (--patch <file> | --icm <program>)`

Documentation reaches main without a workspace, without a gate of its own and without waiting for a land's gate, and
main's tree is never dirty outside the ref turn. The edit is the input, never main's working tree: a patch (`git apply`
input, paths relative to main) or an ICM program (what `icm run` takes). Either file may be `-`, stdin, as may the
message file, and stdin serves one of them at most. Main is the checkout `--project` names, else the working
directory's, when cowshed's in-checkout marker (`.cowshed/workspace.json`, 01_storage.md) gives it no other role: a doc
edit in a workspace is refused, since it is committed there and landed. The verb asks no daemon; its ref turn is the
host gateway's ("Land turns"), or the one `--gateway-socket <path>` names, as for `land-turn`. It needs main's
`.cowshed.toml` to declare `[doc]` (which paths are documentation) and `[land]` (whose `turn` names the ref turn
`<turn>.ref`).

Both inputs are read first. Then, in one hold of the ref turn (FIFO, its holder named, "Land turns"), each command the
hold runs (git, the ICM program, the check, the commit) binds itself to the turn as it starts, so the gateway keeps the
turn until it exits, however this process ends; one the gateway does not bind, one whose stdin cannot be written and one
still running when the turn is lost are sent SIGTERM, nothing is put back while it runs, and the doc commit fails once
it has ended:

1. Main's `git status --porcelain=v1 -z --untracked-files=all` is read: the paths main holds uncommitted.
2. The edit's paths come from the edit's own account. A patch is judged before main's tree is touched: applied to a
   scratch index of `HEAD`, its paths are the ones that then differ from `HEAD`, both sides of a rename, and a patch
   that does not apply to `HEAD` is refused. An ICM program is run as `icm run <program> --root <main> --json` (the
   `icm` on the caller's `PATH`, in main's pure environment), and its paths are the ones whose status it changed; every
   path main held uncommitted is copied aside before it runs, so an edit that writes one is seen and that path put back
   byte for byte.
3. Admission: every path must match `[doc] paths` and must have matched `HEAD` before the edit. A path that is not
   documentation is refused, naming the declaration and the land route for code; a path main held uncommitted is
   refused, since committing it would commit someone else's edit and putting it back would destroy it.
4. `[doc] check`, when declared, runs with the admitted paths appended; a nonzero exit refuses. It may rewrite those
   paths (a formatter), and a path outside them that it writes is refused.
5. Exactly those paths are committed: `git add -A -- <paths>` in main, then `git commit --only -F <msg> -- <paths>` in
   main's pure environment with the repository's own hooks on.

Whatever stops the hold short of the commit puts every path the edit or the check wrote back: a path `HEAD` has is
restored in the index and the tree, a path it created goes, a held path gets its copied bytes back, and the refusal
exits 4; bad usage is exit 2. A `HEAD` that moved means the commit landed before the stop, and nothing is put back. The
index stages a held path is put back with are its own from before the edit, taken in one index read and grouped in
memory by path; putting back reads `HEAD`'s tree once when a path needs it and runs one git batch per restoration kind
with its paths on NUL stdin, a held path's copied bytes going back only after its index stages. One batch answers one
exit, which cannot say which of its paths git applied: a failed batch therefore names its whole attempted partition and
skips only that partition's dependent step, while every other partition still runs; a `HEAD` tree that cannot be read
names the paths it would have classified, and the held paths are still put back. A doc commit that lands while a land's
gate runs is a commit main gained, which that land's step 5 rebases onto and re-gates: a document some gate task hashes
is gated by the next land, visibly, never here. The report is `{commit, paths}`, bare `commit` on stdout without
`--json`.

### Lanes: `fork` and `--into`

A lane is a line of work that reaches main as one change but is built by several workspaces. Its base is an ordinary
workspace, `cowshed fork <lane>` (a clone of main). Its units are `cowshed fork <lane> <unit>`. A unit rebases onto
(`cowshed rebase <unit> --into <lane>`) and lands into (`cowshed land <unit> --into <lane>`) the lane base, then is
removed. The lane completes with the ordinary close-out of its base: `cowshed rebase <lane>` and `cowshed land <lane>`
fast-forward main and remove the lane base.

A unit's fold into its lane base is ungated: `cowshed land <unit> --into <lane> --ungated` rebases the unit onto the
lane base's branch and fast-forwards it, without a check and in one command, and `cowshed rebase <unit> --into <lane>`
is that rebase alone, for a unit that takes its lane-mates' work before it lands. The fold does not run a check, freeze
a seed or adopt a build volume; it refreshes the target shell once before retiring the unit. The lane base's build
volume, link and seed stay as they were, and each unit's build volume is released when the unit retires. The lane is
built once, at its base's own gated close-out into main, whose check is the lane's batch gate; the lane base builds the
lane's whole delta there, incrementally on its own volume, and main adopts that volume (16_build_volumes.md, "Ungated
folds"). Measured 2026-10-22 on SmoothBricks folds into a lane base before folds were ungated: the land spent 68 s in
`freeze-seed` and 22 s in `adopt`, while the Git work took seconds. Without `--ungated`, a land into a lane base is the
full gated close-out above, for a stack whose units each gate (16_build_volumes.md, "Stacks and merge queues").

The caller (an external coordinator) tracks the tree; cowshed stores nothing about lanes. `--into` names a workspace,
and every lane rule is read off that workspace at the moment of the call: its checked-out branch is the target, its
repository is where the unit's commits are fetched and fast-forwarded, and its branch is what the retire measures
against. A standalone `cowshed rm` of a unit still measures against main. The one lane fact cowshed keeps is the land's
own retire request, which journals the lane base and branch it measured against so that its replay measures the same
("`cowshed rm <ws>`", "Removal persists its exact options").

`--into` refuses the unit itself (exit 2), and `--into main` is the default. In the API the lane is a `WorkspaceRef`
rather than a name: the reference carries the incarnation it was resolved at, and land and rebase refuse (exit 4, naming
the recreation) when the lane base's current incarnation differs — a lane base removed and recreated under the same name
is another workspace, and a unit's work never lands into it by name alone. The CLI resolves `--into` to a reference
once, when it runs.

Nothing in the model depends on how a clone is made: a lane base is a workspace and a unit is a clone of it, which is a
clone of a snapshot of the lane base on ZFS as it is a clone of its image on APFS.

## `cowshed rm <ws>` — perceived-instant deletion

1. Refuse (exit 4) unless every local branch (HEAD included) is landed in live main: the image takes every branch with
   it. Landed is judged in three questions, cheapest first. A tip main contains (an ancestor of main, or main itself) is
   landed, cheaply and exactly. Otherwise a tip whose merge into main changes nothing is landed:
   `git merge-tree --write-tree <main> <tip>` merges cleanly to main's own tree, one process for the whole branch, and
   it follows a file the branch renamed, where the per-file proof below sees a new path that main has since edited. No
   merge driver decides it: every driver the repository's config names and the built-in `union` fail, whichever
   attributes select them (committed, `$GIT_DIR/info/attributes` or none), so a file one would resolve conflicts
   instead; `merge=ours` would otherwise drop the branch's side and still reproduce main's tree. The merge writes its
   objects into a scratch object directory it then removes, never into the workspace's object store or main's (a linked
   worktree's store is main's). Otherwise every commit of `main..<tip>` must be held by one of two proofs. **Content:**
   for each file the branch changed since its merge base with main, main already holds that change: main has the same
   entry, or merging the branch's change into main's version (`git merge-file`, based at the merge base) reproduces
   main's version, where a region both changed is held when main's lines there have dropped the lines the branch
   replaced and carry the lines it wrote, in order. **Patch identity:** main holds a patch-equivalent commit, as
   `git cherry main <tip>` marks it `-` (git's patch-id against main's commits since the merge base), for an ordinary
   commit that changes something; a merge that authors nothing (an empty combined diff) is held as well. A file the
   content proof does not hold is unheld while any commit that changes it is held by neither proof. A cherry-pick, a
   squash, a history rewrite, or a pick whose conflict a lander resolved therefore lands the work under other commits, a
   pick main then edited past is held by patch identity, and an empty commit changes no file and carries nothing; a
   squash of several commits or a resolved pick that main then edited past matches no commit's patch and is held by
   neither. A ref in main's repository keeps a tip when the tip equals the ref's commit or is an ancestor of it, and
   HEAD is kept so by `refs/cowshed/<ws>/heads/<branch>` for its checked-out branch, where `push` preserves it: those
   commits live in main's repository, which outlives the image, so a shed pushed for review retires with no flag. A HEAD
   the target holds asks no ref. A detached HEAD is on no branch, so no ref is its to read, and a HEAD ahead of its ref
   (committed since the push, or diverged from it) is kept only as far as the ref goes, which is not the head.
   Uncommitted work refuses first, whatever keeps HEAD, and the refusal names it, ten paths and a count of the rest: a
   tracked change, staged or not and hidden or not, and an untracked file no ignore rule covers. An untracked hidden
   path (one with a `.` component) is junk, and so is an untracked path the target checkout's current rules ignore or
   that it holds byte for byte (or by the same symlink target); a path the workspace's own rules ignore is never work.
   An in-progress Git operation refuses the same way. A branch other than HEAD also passes when its tip equals or is an
   ancestor of main's repository branch of the same name or of the ref `push` wrote for it. The refusal names each
   branch that did not pass with its tip, how many of its commits neither proof holds, each unheld file with why main
   lacks its change (`missing`, `conflict`, `added`, `deleted`, `changed` where main deleted the file, `mode`, or
   `unmergeable` with git's reason), ten files per branch and a count of the rest, and then those commits, each with its
   subject, ten and a count of the rest; `ls --landed` applies the same measurement. This is not the install gate's
   question (05_gateway.md, "The installed cowshed"), which asks whether a build holds the installed commits themselves,
   by ancestry or patch identity alone. `--force` waives neither uncommitted work, an in-progress operation nor unlanded
   work. An explicit `rm` (and a project purge, for each session it removes) also refuses a workspace younger than ten
   minutes, measured from the creation time its record carries, after every other gate has passed: a clean workspace at
   main is indistinguishable from a landed one, and one its owner forked minutes ago may not have its first edit yet.
   The refusal names the age and the floor, and its hint the remedies that keep the work; a creation time that cannot be
   read refuses the same way. It alone carries `removal: {reason: "young", createdAt, removableAt}`: `removableAt` is
   the first second the floor lets the workspace go, absent when the creation cannot be read, so a caller that owns the
   workspace waits for that second instead of copying the floor. `--force` overrides it. A `land` retiring the unit it
   landed or a workspace it folded (`land` step 5), and the replay of a removal already authorized, are not asked.
   Explicit `--abandon` authorizes losing all three. It requires a verified recovery bundle,
   `sessions/.trash/<ws>-<head>.bundle`, holding HEAD and every branch that did not pass as refs, and, when the tree
   holds uncommitted work, one more commit on top of HEAD holding the work tree as it stands, under
   `refs/cowshed/uncommitted` (`AbandonedWork.uncommitted`): each dirty path taken as the tree holds it and one the tree
   lost as its deletion, staged through a private index so the workspace's own index, HEAD and branches are untouched.
   An in-progress operation's own state (`MERGE_HEAD`, a rebase's plan) is not kept; its work tree is. No image is kept
   (step 5). The bundle is written beside that name and renamed into place once verified; a retried removal that finds a
   bundle verifying for the same range keeps it instead of writing another. The bundle is the only copy: nothing is
   written into main's repository, whose refs are the branches people check out and the ones `push` writes, never a
   namespace per abandonment that nobody reads. It stays in the trash until main holds every commit it carries by patch
   identity, when a plain `gc` drops it as `heldBundle`: each commit a tip carries beyond main is one `git cherry` marks
   `-` (and changes something) or a merge that authors nothing, and a tip carrying nothing beyond main is held by
   ancestry; a bundle that advertises no tip is kept. `gc --abandoned` releases every bundle (01_storage.md,
   "Retention"). The report names it (`AbandonedWork.bundle`), and `AbandonedWork.preserved` says what else keeps the
   tips: absent (`bundleOnly`) for every session abandonment; `nothingToPreserve` for main's own `--restore --abandon`
   (below), where main is what is being restored and its bundle is the copy; or `preserved` with each ref, in a record
   an earlier build wrote when abandonments also wrote refs into main's repository, which no build writes or reads now.
   A measurement's files (`unlandedPaths`) are `named`, or `predatesPaths` for a record from a build that counted
   commits by patch identity and named no file. A command reporting a predating record warns once on stderr, naming what
   it does not record (00_overview.md).

   Every retirement, abandoned or not, appends a `retire-workspace` line to the project's `deletion-log.jsonl`
   (01_storage.md) once the source release is committed: the head and every local branch tip read before the image went,
   each with what keeps it — `{"by":"bundle","path":…}` naming the abandon bundle for a tip the abandonment kept,
   `{"by":"target",…}` naming the branch and head that held a tip the gate passed, or, for the head a `push` ref alone
   kept, the ref and its commit (`"branch":"refs/cowshed/<ws>/heads/<branch>"`), which is then the only copy; any other
   tip is named by the branch the gate measured against, whichever proof passed it. The line also says who retired the
   workspace (`retiredBy`): the `caller`, the process that ran the removal with its pid and argv and its parent's, which
   is the shell, script or agent that asked; and, for a forced removal, `forced`, naming each guard the force overrode
   (`young` with `createdAt` and `ageSeconds`, the one guard it overrides), empty when it overrode none. A retirement
   whose tips could not be read says so on stderr and writes the line without Git facts; it is never refused for that.

   The refusal reports whether the published branch already holds HEAD's commits; it never authorizes a removal. Only
   once the gate has refused HEAD does it measure HEAD, with the same landing module `ls` reads, against the primary
   bound remote's tracking ref of main's branch in main's own repository (`refs/remotes/<remote>/main`, as main last
   fetched it; nothing is fetched). When that ref holds every commit HEAD carries, HEAD's finding gains "`<remote>/main`
   (published, `<tip>`) holds all of them" ("holds it" for one commit), then how local main stands against it: "local
   main has diverged from it (`<a>` ahead, `<b>` behind)", or "local main is `<b>` behind it" when local main has no
   commits of its own, the normal gap after a release. It ends with "reconcile main, do not abandon". It speaks for HEAD
   alone: the other branches the refusal names follow it as the gate measured them, and a refusal for other branches
   alone measures nothing published. Otherwise, including when the ref cannot be read, the refusal says only what the
   gate measured. The hint is the same either way.

2. Stop the supervisor. Ordinary removal quiesces, letting running jobs finish before retirement. `--force` retires
   directly: TERM → grace → KILL across the workspace's descendant process trees (11_shell.md), without waiting for a
   job to complete. Teardown precedes image retirement — live children would otherwise hold the mount busy and keep
   enforcing stale launch-time authority after the grants disappear. The checkout's Nx daemon outlives its supervisor
   (it detaches from the job that started it) and its cwd is the checkout, so it is stopped as `detach` stops it
   (06_cli.md). A process cowshed does not run that holds the mount refuses the removal before the supervisor stops,
   `--force` or not, and again once it has stopped ("Holders cowshed does not run", below); step 5's grace and force
   cover only a holder that appears after that second look.
3. Remove the host-side state this workspace put in main's repository, before the image goes anywhere: its reverse
   remote if one was registered, and its worktree registration if it is a git-worktree workspace (remove, then prune).
   This is the one piece of teardown that lives where the user can see it — a remote or a registration naming a trashed
   mount is a broken fetch and a broken `git worktree list` in the user's own checkout.
4. Release the session's owned live build volume and current-incarnation seed before its source image
   (16_build_volumes.md, "Garbage collection"). A volume adopted by the target belongs to that target and stays. A held
   volume or failed release refuses while the source image and unfinished retirement journal remain.
5. **Release the source: commit the release in this operation, free it in the background.** After the final
   dirty/in-progress, HEAD-stability and all-branch containment fences, detach the exact source and remove its scoped
   temporary directory and empty mountpoint through the existing retirement checks. Then commit the release with one
   rename of the canonical source image, never replacing a file, to `sessions/.deleting/<ws>-<incarnation>.dmg` and sync
   both directories, opening no byte of the image: before the rename the workspace is whole, and after it nothing
   mounts, forks, fences or reads the image again. Right after the rename is durable the removal nudges the gateway with
   one one-shot `released` request that returns at once (05_gateway.md, "Deleting released images"), removes the CA
   companion, grants sidecar and facts and answers; it frees no byte of the image and never waits for the free
   (16_build_volumes.md, "Garbage collection"), so no reply of this verb waits for source-image bytes, and until the
   gateway's cleanup has freed the entry, the store's container still holds the image's allocation: free space lags the
   removal. A gateway that is absent, does not answer or refuses is said once on stderr and fails nothing, since the
   rename is the release: the entry waits for the gateway's next listing. A failure before the rename refuses with the
   owner metadata and mutating intent kept for the same removal to retry. `sessions/.deleting/` holds released images
   waiting to be freed, not a crash journal: a crash after the rename leaves the entry, which nothing names, for the
   gateway's next start, and the next lifecycle open or recovery reconciles the record the rename left imageless without
   waiting for the entry or freeing it, because the reconcile of a canonical sidecar whose image is gone never depended
   on the bytes being freed. The removal's retry finds the workspace already gone. An open confined to other workspaces
   neither reads nor waits for it.

   Every retirement releases its image; none keeps a recovery image. What could survive only in the image is refused by
   step 1 unless `--abandon` bundles it: uncommitted work, by the dirt rule there, and unlanded commits. What that rule
   reads as junk goes with the image: a path ignored by the workspace's own rules (`.gitignore`, `.git/info/exclude`,
   the user's excludes file) or by the surviving target checkout's current rules, an untracked hidden path, and an
   untracked path the target holds byte for byte. So do cowshed's own `.cowshed/`, the volume's bookkeeping and the
   build-state links, which each checkout's `.git/info/exclude` ignores. An unfinished clone's retirement, a rename's
   source once its destination holds its head and work, and `rm main --force`, which throws the warm main image away,
   release the same way.

   A `sessions/.trash/<ws>-<incarnation>.dmg` an earlier build kept as a recovery image is garbage. A plain `gc`
   releases it into `sessions/.deleting` as it finishes any retirement: with its workspace-scoped artifacts (temp dir,
   empty mountpoint, grants sidecar, CA companion) when its grants sidecar names it, and as its own files alone (the
   image, its CA companion) when that sidecar is gone, since a missing record names no workspace's environment. An
   opening never releases one: only `gc` does, under its plan's locks. That first `gc` after the upgrade is the one-time
   cleanup of every such image.

The in-progress Git gate follows operation state (`MERGE_HEAD`, `CHERRY_PICK_HEAD`, `REVERT_HEAD`, `rebase-merge`, or
`rebase-apply`), not a leftover `.git/AUTO_MERGE` tree: Git can retain that tree after an operation finishes. A stale
tree alone never blocks removal.

The verified bundle's temporary bare repository belongs to the removal attempt, not the next `gc`: cancellation or an
interrupted verification removes it before leaving the operation. So does the `.partial` bundle it stages beside the
final name: the attempt renames it into place or removes it, cancellation included, since no sweep ever owns one.

Removal persists its exact options before the authorized retirement mutation and records completion only after the
release is committed. If a process dies after the canonical rename or retirement but before acknowledging the call,
startup observes the workspace absent and records completion without retiring it twice. A prepared request that never
crossed the safety fence is discarded, not a deferred deletion; an authorized mutating request reruns the safety fences
and retirement. Cancellation cannot discard the journal while the blocking release commit finishes.

The request also records what the retirement measures against: nothing for main's `main`, which every `rm` measures
against and every record from before the field means, and for a `land`'s retire the workspace and branch the unit landed
on ("Lanes"). A replay measures what its request measured, so a lane unit whose land delivered but whose retire was cut
off is retired against its lane base, never refused against main. A named `rm` first replays its target's own unfinished
retirement; one that replay cannot finish — a refusal, an unreadable target — is reported on stderr and superseded: the
`rm` journals its own options and containment (main, as any standalone `rm`) before its first mutation and reruns every
fence under them, so a journaled request's refusal never stands between the caller and an explicit removal, and
`--abandon` authorizes destroying what main lacks. A removal re-issued after a session's retirement finished, by a
replay or an earlier call, answers with that retirement's report, whichever options either carried.

A named `rm` can retire an unfinished fork without activating it. Its supervisor, if one runs, is stopped first, before
the clone's image lock is taken, as an active workspace's `rm` stops one: the stop waits for the clone's jobs, and
nothing waits for them under the lock. The exact pending image is then mounted and validated under its image lock, and
receives the same dirty-state and landed-work checks as an active workspace, read after its only writer stopped. A
refusal leaves its supervisor stopped, which only the clone's publication starts again, and says so; it preserves the
image and original clone intent and leaves its mount state as it found it: an image the `rm` attached to inspect is
detached again, and one it found mounted stays mounted. Once retirement is authorized, its journal record retains the
original clone source so an interrupted removal can recover. The clone's owned volume and seed are released before its
source, through the same direct-release or recovery-preservation choice, before answering.

Removal-scoped startup never finishes the named target's own unpublished clone: it leaves the clone intent for the `rm`
to retire, so whatever stopped the clone — a duplicate port block, a start revision the source no longer holds — cannot
also stop its removal. An unfinished clone intent that left no pending image is retired the same way, with only its slot
binding to release; under the intent lease, so a live creator is refused. Only the named target is retired; its peers
keep their grants. The retired image is validated by its exact trash path, repository, format and incarnation; being
unpublished before retirement does not invalidate that recovery fact.

An unfinished fork whose source workspace no longer exists can never finish, so recovery that would finish it (a `gc`'s
store-wide pass, or any opening whose scope holds it) retires it instead of leaving it journaled for a later pass: its
pending clone exactly as a named `rm` retires one, forced as an abandoned clone's retirement is, and an intent that left
no clone with only its slot binding to release. A retirement refused there, say for history main lacks, keeps the fork
journaled with that refusal. Kept journaled with no refusal of its own, such a fork held its clone, and every build
volume it registered, deferred behind it on every pass.

`cowshed rm main --restore` is the adoption rollback described above and maps to `RemoveOptions.restore`; plain
`cowshed rm main` requires `--force` and a clean `git status` (exit 4 otherwise).

`cowshed rm main --restore --purge [--force] [--abandon]` (`Coordinator::remove_project`) removes the whole project in
one call: every session workspace, including clones a create never published, under the given `--force`/`--abandon`;
each removal has already committed its release when collection begins; collects; with `--abandon` deletes the abandon
bundles in `sessions/.trash`, the only copy of the commits they hold, which `--abandon` authorizes losing
(01_storage.md); and restores main, whose final unbinding moves the project's released images into the store's
`.deleting` and nudges the gateway ("Checkout layouts"). Each step is idempotent, so a refusal leaves the rest to the
same command. A collection that still finds the store changed under it (another process) is refused as retryable —
`Conflict` with `Retry::GcPlanStale` — and the same command finishes the removal; callers never sleep and retry `gc`
themselves.

## `cowshed attach` / `cowshed detach`

Explicit attachment lifecycle for long-lived workspaces: `detach` first stops admissions and drains the supervisor and
Linux connector, unlinks the Linux per-incarnation socket, then frees both of the workspace's attachments: its image and
the build volume its checkout owns (its record names the checkout as the one that links it), detached whole under the
build-volume claim "Making room for an attach" describes. The images/dataset and grants persist. A build volume a job
holds, or one the kernel will not unmount, refuses the detach by name after the workspace's own image is released; it is
mounted again before its claim is let go. The refusal is a conflict, never a storage failure: it names a job admitted on
the volume when the volume's hold file is held, otherwise each process that has the volume open ("in use: it is open in
pid N (command)"), otherwise that no process of this user holds it, and its hint is to end that holder or wait, with
`sudo lsof +f -- <mount>` as the next look for an unseen one ("When a detach's unmount is still refused"). The
workspace's own jobs are not such holders: the drain above waits for them first. Every path that detaches a workspace
releases both: `detach`, making room, and putting a detached workspace back after a refused `rm`. `attach` mounts at the
canonical path with canonical flags, mounts the linked build volume again and, on Linux, creates the per-workspace
socket/netns and starts exactly one connector before admitting execs. On macOS a session attaches on use: the gateway's
startup pass mounts mains only (05_gateway.md, "Startup contract"), and every verb that reaches a detached session
attaches it; Linux uses its platform service/controller lifecycle. An explicit `attach` of a detached workspace, like a
fork, never waits for gate-class work (05_gateway.md, "Host scheduling"); an attach of a mounted workspace attaches
nothing.

### Making room for an attach

IOKit queues at most 1024 matches on one unfiltered IOMedia notification and drops the rest (01a_apfs.md, "The IOMedia
match-notification ceiling"). Cowshed attaches every image in the kernel, which waits on no such notification and is
measured to attach past it, so the ceiling is a soft one: nothing refuses an attach on the media count, and an operation
that may attach first makes room where it can. The project actor does this for each controller operation once the
operation's route has authorized and validated its request, so a refused request has no effect on the host
(`cowshed_core::media_room`); the CLI, N-API clients, the gateway and a coordinator's `cowshed controller` get the same
step, and `mount main` makes the same call before it attaches a detached main. It counts the registered IOMedia,
subclasses included; three for each image attach running on the host, any user's; and 16 kept for disks the host
attaches on its own, and makes room when the images would bring that to 1024. A test namespace counts only the IOMedia
the images attached from its own store register, with the same 16 kept (01_storage.md, "Host namespace"), so another
user's attach never moves a fixture's decision. `doctor`'s `media-notification-cap` reports the same count, what it
commits of the ceiling and how many more images fit: a warning while fewer than 20 do, past the ceiling too, never an
error.

An operation's demand is read from its request alone (`cowshed_core::media_room::attach_demand`). One that works in a
workspace (attach, exec, a shell, a push, a land or rebase, a grant, a defragment, a rekey, an Nx state clear, a worker
handle, the build-state refresh `cowshed setup` runs) attaches it and its build volume, two images, when it is detached,
and its build volume alone when only that is or when its checkout links none yet, since the build-state refresh may mint
its first (16_build_volumes.md, "One link per checkout"): a mounted workspace's linked build volume is read from its
build link, and both mounts from the kernel mount table, so an operation in a mounted workspace with its build volume
never reads the IOKit count. A fork or rename attaches its destination the same way: new, and so detached, unless a
resumed one is already mounted. Adoption attaches main and its first build volume; a resize one build image or seed
beside the workspace's own; moving the checkout or changing the repository identity main. Reads, job and session
traffic, `rm`, `detach`, `gc`, `doctor`, `land --preflight` and project-wide grants attach nothing. The workspaces an
operation names (a fork's or rename's destination and source, a land's or rebase's `into`, main when it names none) are
its own and never detached for it. A host whose count leaves room is read once, and nothing else happens; an `exec` of a
mounted, served workspace answers before any of this (06_cli.md, "Resident workspaces").

Three paths attach without making room. Attaching every detached session (`attach` with no name, `attach --all`) is an
aggregate: detaching some sessions to attach others frees nothing, so each of its attaches says so in its request
(`AttachOptions.aggregate`), and the store-wide `attach --all` mounts through the substrate directly. The replay of an
unfinished lifecycle operation when a project opens finishes an operation already admitted. The gateway's startup heal
mounts mains only (05_gateway.md, "Startup contract").

When the images would not fit, the verb lists every adopted project's attached workspaces and plans
(`cowshed_core::media_room::plan`, a pure function of one media reading and that list): only idle workspaces are
detached, the least recently used first, and only as many as free the excess; when every idle one together frees too
little, none is. A workspace is idle by its supervisor's own definition: no supervisor serves it, since one retires only
after 30 minutes with no named session, no job and no Nx daemon (11_shell.md, "Supervisor"). Never idle are a project's
main, the workspaces the verb names in its own project, a workspace whose supervisor serves it or whose supervisor
socket lease a lifecycle operation holds, and one a land or an `rm` holds. When a workspace was last used is the latest
modification time of its images' backing files, which every write to a mounted volume moves. Each detach takes the
workspace's supervisor socket lease without waiting, so no supervisor starts on it meanwhile, and its land lease
(`make-room`, "A land and an rm of one workspace exclude each other"), so no land or `rm` of it starts either; either
lease held elsewhere makes the workspace busy. It then makes the unforced unmount `cowshed detach` makes, which
revalidates the mounted volume's marker against the workspace's incarnation; a process holding the mount refuses it, and
the workspace then counts as busy and the plan is made again without it. The detach is the one `cowshed detach` makes,
so it frees the build volume with the image (above): only a volume whose record, read again under its claim, still names
this workspace as the checkout that links it is detached, so a volume a land gave another checkout since is left as it
is. A build volume that will not detach is said on stderr and the workspace counts as unreleased. Each detach is said on
stderr, and the media are read again after each round, until the images fit or nothing idle is left. The gateway's view
of each project a detached workspace belonged to is reconciled. A detached workspace attaches again, with its build
volume, on its next use, as any detached workspace does.

When nothing idle frees enough, nothing is detached, a warning on stderr says so, and the operation attaches anyway. The
warning names the count, the excess and the attached workspaces, ten of them with the IOMedia each holds and why it
stays (idle, main, the verb's own, its supervisor serves it, a land or removal holds it, a process holds its mount, its
detach failed), then how many more; its next step is to stop the work in a workspace one is done with and
`cowshed detach` or `cowshed rm` it. A media count or a store listing that cannot be read is a warning the same way:
making room never fails the operation it makes room for.

### Holders cowshed does not run

A workspace is in use while a process cowshed does not run holds its mount: the process's working directory, root
directory or executable is under the mount, or it has a file there open. Event-only opens hold nothing (FSEvents,
Spotlight, cowshed's own shell watches). A host shell's `direnv exec <ws> nx …` is such a process. It runs with no
supervisor and no job, so the supervisor's view alone calls the workspace idle. On 2026-10-21 a census of idle sheds
that went by supervisors alone missed at least nine running Nx runs; adding the working-directory and open-file checks
found them. Such work belongs in `cowshed exec`, whose jobs the supervisor's ledger records; the census is no way of
keeping it alive.

Cowshed runs three kinds of process on a workspace, and only these are exempt: the supervisor's process tree, the job
groups its ledger records (11_shell.md), and the Nx daemon the checkout's own record names, with its plugin workers. The
census (`runtime::mount_activity`) takes one pass over the process table (`build_volume::nx::tree_holds`). For each
process it checks the working and root directory, then the executable, then each vnode descriptor, and keeps the first
hold it finds. For each holder it then walks up the parents and reads the ledger once. It reads kernel process state and
stats nothing on the mount but its root. A holder whose parent the kernel will not describe to this user is not shown to
be cowshed's, so it stays a holder.

A census asks nothing of a process a cowshed lifecycle operation runs in. The workspace's intent lease
(`sessions/<ws>.intent.lock`, held for a create, fork, rename, adoption or retirement) and its land lease
(`sessions/<ws>.land.lock`, held by a land or an `rm`) each record their holder's pid and kernel unique id
(`p_uniqueid`) in the lease file, written after the holder takes the lease's `flock(2)` and emptied before it lets go
(`lease_holder`). A pid is passed over only while a lease file of the census's own store names it with the unique id its
process has now: a record that names no process, or whose pid another process has since taken, names nothing. This is
positive evidence the holder wrote under its own lock, not a guess from the process tree. Such a process can sit in one
kernel call for minutes holding one of its descriptors' vnodes (a fork's first mount of its clone, "`cowshed fork`" step
3), and it is no foreign holder: the operation's own leases and image locks already refuse the operations a census
guards. The census's own process is always asked, whatever its leases say. A verb that retires, detaches or moves a
workspace first moves its own working directory off that workspace's mount, to main's checkout, so the directory it was
invoked from never refuses its own retirement; anything else of its own there, an open file, still holds the mount and
is named like any other holder, and so is the working directory of the shell that invoked it, a process cowshed does not
run. Every census on macOS starts from this one list of pids, the volume census included, which walks the processes
itself rather than through libproc's `proc_listpidspath`. A census serves one store, the one its caller's roots name
(`census::CensusScope`, from `CanonicalRoots::census` or the storage host's), and reads that store's leases
(`<store>/<owner>/<repo>/sessions`) and no other store's. Which pids it asks follows the store's namespace. The host
store's census asks every process the kernel lists: what it exists to find is a process cowshed does not run. A test
fixture's store is reached only by the process that minted its namespace, the processes in its process group, and the
descendants of either, by the parents and groups the kernel lists (`PROC_PIDT_SHORTBSDINFO`, one call per process that
reads no vnode), so its census asks those alone. A fixture's stand-in for a host shell's run, which it backgrounds from
a shell that exits, stays in the group though launchd adopts it, and is asked as that run would be; a process that made
a session of its own and was adopted, such as a detached daemon, is outside the fixture. call that process sits in, and
never reads the host store's leases. Measured 2026-10-28: an in-process fixture census read 740 host leases in 5 s and
its descriptor walk waited on unrelated host processes held in APFS calls, holding `command_dispatch` substrate tests
past their 180 s progress bound. `census::asked` makes the decision from the listing and the leases alone, with no I/O.
Linux records no unique id: no lease passes a process over there, and its census asks every process in its reach.

A record's unique id is its exact decimal string (`DecimalU64` in `api::held_lock`, the form every kernel identifier on
the wire takes), the one form any reader parses; the switch from a JSON number was a clean cutover, with no reader of
the old form. A record an earlier build wrote, with a numeric unique id, does not parse and names nothing: none takes a
pid, an exemption, an owner or a signal from it. What such a record costs is observation and time only. A census asks
the process it names like any other, so that one query may wait as long as the process's kernel call (above), and the
process may be named a holder and refuse the operation; a land lease held under it refuses its contender without naming
the holder; `lock-holders` proves no owner from it, so nothing is signalled. Every such record is ephemeral: it lasts
only while its writer holds the lease, or, left by a writer that ended without unwinding, until the next holder rewrites
the file, which is why the cutover keeps no converter.

A holder check belongs in front of a destructive operation and, but for the one exception below, nowhere else: once per
operation, in that operation's own context, never on an interval and never inside a supervisor's actor. `detach`, `rm`,
the retirement a `land` ends with (which is `rm`'s), `mv` and gc's retirement of an abandoned unpublished clone consult
this census. Each asks before the supervisor stops, with the serving supervisor exempt. Stopping the supervisor first
would end the keeper's Nx daemon that a host run may be using, so the census itself would disrupt what it measures. Nor
may a process of the keeper's own come or go while the census reads the process table: a start that exits before the
census walks its parents is not shown cowshed's, and on 2026-10-25 a land gate's `rm --force`, issued as one failed
start was followed by the next, refused over that `start.js --kept`. So each verb first asks the supervisor to stop its
keeper's starts and to end the start it has in flight as the job it is, answered once that job concluded
(`WorkspaceSupervisorHandle::stop_nx_keeper`). A start whose daemon the checkout's record names live is serving and is
left, exempt by that record. A refused verb leaves the keeper stopped until the shed's next job or session, as an idle
shed's keeper starts none until then (04_sandbox.md, "The Nx daemon stays on"). A retirement asks again once the
supervisor has stopped, before the image is retired. A held mount refuses with `conflict`, naming each holder's pid,
command, and what it holds where (`pid 4242 (nx run-many -t test), working directory /…/raven/packages`). It names ten
holders and counts the rest. No holder is stopped, and no flag overrides the refusal (11_shell.md, "Teardown ordering").
A supervisor's idle retirement and the Nx keeper's daemon stop (04_sandbox.md) run on timers and consult the
supervisor's ledger alone: a supervisor retires once its ledger has held nothing for its idle bound, whatever else is on
the mount, since retiring it unmounts nothing. gc's build-volume collection is a separate gate with its own grace
(16_build_volumes.md) and does not consult it.

One census is admitted outside a destructive operation, and only this one: the head of a land-turn queue waiting for its
`--outside-lock` names the processes with that lock file open (05_gateway.md, "Land turns"). It runs on that
`cowshed land-turn` process's own thread, one walk when the wait starts and one per 90 s report while it lasts, never in
a supervisor or daemon, and it decides nothing: the head waits for the kernel's lock alone, and the census only says who
has the file open. The exception widens to nothing else.

When a detach's unmount is still refused by the kernel, the refusal names what holds the volume: first the tree census,
then the volume census, which also walks mapped files. When neither names a process of this user, the refusal says so
and names any file system mounted inside the volume. With none, the holder is another user's process (Spotlight's `mds`
runs as root) or the kernel, and `sudo lsof +f -- <mount>` is the next look.

`cowshed tree-holders <dir>` reads the tree census out for a caller that is not cowshed: one JSON line per process of
this user holding something inside the tree, each a `TreeHolder` (`api::tree_holder`, read with its generated validator)
with its fields in this order: `pid`; `hold`, the first hold the census found (`working-directory`, `root-directory`,
`executable`, `open-file` or `mapped-file`); `path`; `command`. A shell reader may match the
`{"pid":<n>,"hold":"<kind>",` prefix. `path` is the held path's exact bytes in the tagged-bytes form `CommandArg` uses:
`{"encoding":"utf8","data":<text>}` when they are UTF-8, `{"encoding":"base64","data":<canonical standard base64>}` when
they are not, so no path is refused, mangled, or the cause of a cut listing. `command` is the holder's argument vector
joined by spaces, each argument decoded lossily: it is for a person to read and is never an identity, which the pid is.
A census that cannot be taken writes no line and exits non-zero; a line stdout refuses ends the listing with a non-zero
exit naming the failure. A repository's land tooling asks it whether its own callers would hold a workspace the land
retires, the same census the retire refuses with.

### Descriptor holders: `cowshed fd-holders`

`cowshed fd-holders [--device <0x…>]… [--inode <n>]…` names every process with a descriptor on one of the kernel objects
named, each named as `lsof -F di` prints it and as `hold`'s `started.output` names a held leader's stdout and stderr
(11_shell.md, "Held leaders: `cowshed hold`"): `device` the address Darwin names a pipe end or a socket by, `inode` the
inode of a file and, on Linux, of a pipe or a socket, whose two ends share it. A watchdog whose step's leader exited and
whose output has not ended asks it who still holds that output, a descendant that left the step's group included. It
prints one JSON object, an `FdHolders` (`api::fd_holder`, read with its generated validator): `holders`, one
`{pid, uniqueId, command, descriptors, matches}` per process, and `uninspected`. `uniqueId` is the kernel's identity of
that process's life, as a decimal string: its unique id (`p_uniqueid`) on Darwin, its start time in clock ticks since
boot on Linux. `descriptors` are its descriptors on the objects, ascending, and `matches` the objects they are on, each
once. `command` is its argument vector joined lossily, as `tree-holders` prints it. An inode is matched by its number
alone, as lsof's `i` field names it: on a host with several file systems, a file of the same number on another one is
named too. A descriptor that names neither field, a revoked one (lsof's `(revoked)`) or a kqueue, names no object.

The census walks every pid the kernel lists once, a lifecycle operation's included, and reads only the descriptor kinds
the objects need. On Darwin those are `proc_pidinfo(PROC_PIDLISTFDS)` and one `proc_pidfdinfo` per pipe, socket or file,
the reader `started` names its own descriptors with. On Linux it reads each `/proc/<pid>/fd` entry's inode. A process
found holding one is confirmed: its life is read, its descriptors are read again, and then its life again, so what is
printed is that life's, never a later process's given the pid. A process that exits during the census holds nothing. A
process whose descriptors or life the kernel withholds from this user (another user's, unless it runs as root) is
counted in `uninspected` and never reported as holding none. Any other failure writes nothing and exits non-zero. It
names no project, asks no daemon, refuses `--json` and observes only. `host.fdHolders` answers the same object
(07_api.md).

## Held locks: `cowshed lock` and `cowshed lock-holders`

A held lock is an exclusive `flock(2)` on a caller's lock file, the one primitive behind `land --ref-lock`,
`land-turn --outside-lock` and `cowshed lock <path> [--state-fd <n>] -- <cmd…>`, through which a repository's own
scripts take their ref and gate locks. The lock is on the inode the path names, and no holder ever unlinks, renames over
or otherwise replaces the file: a holder that unlinked it would hand a waiter already blocked on the old inode a lock on
a file no path names while the next caller created and locked a new one. The holder's record is the file's content, so
recording and clearing it truncate and rewrite bytes in place on the locked inode; the inode, and the lock and queue on
it, stay. A holder that finds the path naming another inode once granted refuses rather than holds it.

- **The holder records itself.** Once the kernel grants the lock, the holder writes the lifecycle leases' record onto
  the inode (`{pid, uniqueId, since, command}`, "Holders cowshed does not run"), and empties it before it lets go, so a
  record outlives its hold only when its process ended without unwinding. `command` is the holder's own argument bytes,
  each argument decoded lossily and joined by spaces, so an argument that is not UTF-8 never stops the record; it is for
  a person to read, never an identity, and nothing a held command runs is taken from it: the command's argv is passed
  byte-exact. The record is a claim, never proof: a reader believes it only as far as the kernel confirms it.
- **The command holds the lock with cowshed.** `lock` hands its command the lock's descriptor, and `land-turn` its
  outside lock's (its turn it binds by pid, 05_gateway.md "Land turns"): the lock belongs to the open file description,
  so the kernel releases it only once cowshed, the command, every process the command handed the descriptor to and every
  short-lived copy the spawn's PATH search made, which the kernel closes moments after the spawn returns, have closed
  it; a waiter's grant, never a probe at the spawn's return or at the command's exit, proves the release. Killing
  cowshed alone never lets another holder in while its command runs. A waiter killed before the grant takes its pending
  request with it.
- **No daemon cowshed launches holds it; the command's descendants do.** Every daemon cowshed launches (a workspace
  supervisor, through the supervisor manager's program spawner; a job; the kept Nx daemon inside one) is spawned through
  the descriptor seal, which marks every inherited descriptor close-on-exec but standard input, output and error (and a
  supervisor's report descriptor), and no held command is their parent. A launch whose inherited descriptors cannot all
  be sealed (on macOS, a census of them past the seal's fixed 4096-descriptor ceiling) is refused at that stage
  (`PrepareChildDescriptors`), never started unsealed. Every held command (`lock`'s, `land-turn`'s, and the git under
  `land --ref-lock`) is spawned through one boundary that hands it the descriptor with two policies.
  `NX_DAEMON_START=never`, the policy every job runs under, keeps patched Nx (04_sandbox.md) from starting a host daemon
  outside the held command's lifetime (the only Nx daemon cowshed runs is the kept one); a stock Nx ignores the variable
  and is not stopped by it. Whether a runtime hands the descriptor to its own daemon or plugin workers is a runtime
  implementation choice, not an OS-wide guarantee. The real Nx control requires a daemon without the policy and none
  with it, then proves the lock becomes available after the owned processes end. A separate direct-command control
  proves descriptor handoff: only the command handed the descriptor keeps the lock after its parent closes it, and the
  lock becomes available when that command exits. `gc.auto=0`, `maintenance.auto=false` and `core.fsmonitor=false`, as
  `GIT_CONFIG_COUNT` entries appended after the caller's, turn off exactly git's automatic gc, automatic maintenance and
  fsmonitor daemon in every git the command or its hooks run. Nothing wider is promised. Any other process the command
  hands the open file description to that outlives it, an unmanaged daemon for one (a git credential helper's cache
  daemon is a possible case, not a measured one), holds the lock, by design, until it closes it or exits; cowshed never
  unlocks around it, and `lock-holders` names it. The policies apply to the held command alone: the git and Nx a
  workspace runs anywhere else keep their configuration, so deferred gc and maintenance run at the next git outside the
  lock. Each path has a real-boundary test: the seal; git's own config resolution under `lock` and `land-turn`; a real
  Nx client (`nx show projects`) handed a held lock, beside a control run without the policy; a fast-forward whose
  target's automatic maintenance detaches (a worktree-prune task held on a FIFO gitdir), with a held and an unheld arm;
  and the fence's owner killed while its real git is blocked writing, with a contender refused until that git ends.
- **The waiter's own word.** `--state-fd <n>` gives the caller the waiter's own word: one `LockWaiterEvent` per line, as
  JSON (`{"kind":…}`), so no reason or path breaks a line, and the caller reads it with the generated validator of the
  same type. A valid state descriptor is adopted before any runtime or setup work, so every failure after that and
  before the command starts is a `failed` line (with its reason); a descriptor that cannot be adopted has no channel yet
  and is said on stderr alone. An invalid `GIT_CONFIG_COUNT` is refused with `failed` before `queued`; an empty one
  counts as zero, as in git. `queued` (with its inode) while another holds it; `acquired` (with its inode) once granted,
  the command's spawn has succeeded, and the controller has relinquished its copies of command stdin/stdout/stderr.
  Those streams belong to the command alone: their EOF does not wait for the controller, including when the command is a
  hold that the caller releases only after output ends. After a successful handoff the state channel stays open until
  the command ends and is never inherited by it. A failed handoff emits `commandFailed` and closes the channel before
  joining, so a caller can release a command-side hold; the controller retains child and lock custody while it waits. A
  later wrapper fault is also `commandFailed` with its reason; unlike a pre-spawn `failed`, it does not claim no command
  ran. A normal child exit, including nonzero, leaves `acquired` as the last line. If the state reader closes, a nonzero
  wrapper exit remains observable; command stderr is not retained as a fallback. Without a state channel, ordinary
  stderr diagnostics remain owned until the controller exits. Signals cowshed receives once it holds the lock are
  forwarded to the command, as `land-turn` forwards them.
- **Who holds it is the kernel's answer.** `lock-holders <path>` prints one JSON object, a `LockObservation`: the path,
  the inode, whether the kernel shows it exclusively locked, the record as written, the owner that record names once the
  kernel proves it, the request (`holding`) a proven owner's record names when it names one (every image lock's does,
  below), and every process with a descriptor on the inode with its custody. The path and the record are their exact
  bytes in the tagged-bytes form `CommandArg` uses (`utf8` with their text when they are UTF-8, canonical standard
  base64 under `base64` when not), so neither is refused, decoded lossily, or the cause of a cut object; the owner is
  judged from the record's bytes, and a record that does not parse as a holder record proves no owner. Linux exposes
  custody per descriptor: a readable `/proc/<pid>/fdinfo` carrying a granted `flock` for the exact device and inode
  proves `holds`. A readable descriptor without that grant proves only `open`; an unreadable candidate makes the process
  `unexposed` unless another descriptor proves the grant, never `open` by default. Disappeared descriptors are omitted.
  The owner is the record's pid holding a proven grant. Darwin exposes no lock state per descriptor (`proc_pidfdinfo`
  carries none, and `F_GETLK` answers a `flock` conflict with no pid): the life the record names, proven by its kernel
  unique id and a descriptor on the inode, is the owner (`recorded`), and every other descriptor on the inode is named
  `unexposed`, never counted a holder. On Darwin no descriptor of a process a cowshed lifecycle lease names is read
  ("Holders cowshed does not run"): a fork's first mount of its clone holds the image's vnode for minutes, and every
  per-descriptor query of the fork, `lock-holders`' included, waits that long. When the record names such a process, its
  kernel unique id alone proves it the owner; any other such process is not listed. `fd-holders` counts them in
  `uninspected`. A process of another user whose descriptors this user cannot read shows none; `locked` still says the
  inode is held.
- **An image lock is a held lock.** Every lifecycle lock a store takes through its image-lock acquisition
  (`main.dmg.lock` and `sessions/<ws>.dmg.lock`, 01_storage.md, the store's template lock among them) carries the held
  lock's record and, beside it, the request the lock is held for:
  `{pid, uniqueId, since, command, operation, workspace}`. `command` is the taker's own argv, which names the process (a
  `cowshed fork`, or the controller or gateway serving one), never the request. `operation` (`LockOperation`: `fork`,
  `mount`, `land`, `gc`, …) and `workspace` (the workspace it makes or changes, a fork's new one though it holds its
  source's lock too; `null` for a step of the whole store or project) are typed and passed by the call that takes the
  lock, the controller's included, never parsed from argv. The taker writes the record once the kernel grants each lock
  and the path is proven, by a walk from the store root that follows no symlink, to still name the inode locked; a grant
  on a file since removed or replaced is refused, unrecorded, as a held lock refuses it. It empties the record before it
  lets go. `lock-holders` on one names its owner by pid and command and, once the owner is proven, its `holding`
  (`{operation, workspace}`). A taker that finds such a lock held and waits for it says so before it queues, as a
  lifecycle span (13_telemetry.md, "Latency spans"): `cowshed: image-lock wait <path> start`, then
  `cowshed: image-lock wait <path> holders=<who>`, the one line of who holds the lock as `lock-holders` judges it when
  the wait begins (one look per wait, as a land's ref lock wait), the owner's request among it
  (`pid 4242 (cowshed controller) holds it, as its record says, for fork of raven`), and once granted
  `cowshed: image-lock wait <path> done elapsed=<waited> status=ok`. An uncontended take, and a try that finds the lock
  held, says nothing. A purge counts a recorded image lock a lock, as it counts a recorded lease. Measured 2026-10-23: a
  fork's clone waited 532.9 s on `main.dmg.lock` behind four forks and an attach while `lock-holders` answered
  `owner: null`: no image lock recorded its holder, and no line said what the clone waited for.
- **Only an owned, proven owner is ever signalled.** `lock <path> --owned-pid <pid>` (repeatable) names the lives the
  caller owns; the waiter holds each by the kernel's identity of its life (a pidfd on Linux, its pid version on Darwin)
  before it queues. On `SIGUSR1` while queued it proves the owner of the inode it is queued on from `lock-holders`'
  judgment (a file renamed over the path since is another lock, and proves nothing), and signals it with SIGTERM only
  through a captured life that has not ended: the signal reaches that life or nothing, never a later process given its
  pid, and an `unexposed` or unproven descriptor is never signalled. It answers on its state channel. `signalled` (with
  the pid) is its refusal of its step and its last word: it never says `acquired` nor runs its command, and exits with
  the conflict code, so a lock the owner gives up is not spent on a step being cancelled. `notSignalled` (with its
  reason) keeps its place in the queue. A grant ready together with a request is taken first, and a request after the
  grant is never answered, so a waiter never says both `acquired` and `signalled`. `lock-holders` only observes. A
  `lock` command forwards the signal to its command, so the lock goes only when that command ends.

## Workspace environment: `.cowshed/env`, optionally sourced by direnv

Every workspace image carries its environment in the in-image private namespace as a plain `source`-able file:
`.cowshed/env` exports, on macOS, `COWSHED_PORT_BASE` and `COWSHED_PORT_BLOCK_SIZE` from detached metadata's exact
`portBlock`; Linux has no port block and omits both lines. In an Nx checkout it also exports the project cache route's
endpoint and read-only flag, `NX_SELF_HOSTED_REMOTE_CACHE_SERVER` and `NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY=true`
(03_caches.md); the route takes no credential. On a host with a provisioned build-cache volume it exports that volume's
root as `COWSHED_BUILD_CACHE` (01a_apfs.md, "Dedicated build-cache volume"), the one spelling every consumer reads; a
host without one omits the line. It never exports the bare token: a file every shell of the workspace sources hands its
exports to every child, and `.cowshed/workspace.json`, not an environment variable, marks a workspace. One function
derives the file from the workspace's recorded platform and block, and it is the file's only writer: create and fork
publish it as they mint, and every supervisor start publishes it again, so values are always current — a workspace
minted before a variable existed gains it on its next start. The supervisor start alone also exports the sandbox's
`TMPDIR` and `COWSHED_RUNTIME_LINK`, the workspace's short runtime link (04_sandbox.md): a mint may run at a staging
mount, and the link is named after the mount the workspace is served at. Nothing is derived from cwd, guessed from a
slot, or trusted from a marker alone.

Shell hooks are optional repository input. Cowshed never creates or rewrites `.envrc` or `.envrc-local`, and adoption
and fork do not require either file or any ignore rule for it. A repository with no shell configuration uses the
ordinary sandbox environment; a project-owned `.envrc` needs no cowshed-specific loader.

Projects that want interactive direnv shells to receive the published workspace values may add this optional line to
their own hook:

```bash
source_env_if_exists .cowshed/env
```

Agent processes receive the same workspace values at spawn from the supervisor (04_sandbox.md), without sourcing this
file or requiring direnv. No verb exists to print these exports on demand.

**An unserved workspace is served before it is entered.** The served file is the workspace's readiness record. A
supervisor start publishes it only after the start's mount has settled the checkout's build link (16_build_volumes.md,
"One link per checkout"). A workspace whose `.cowshed/env` still lacks `TMPDIR` or `COWSHED_RUNTIME_LINK` has not been
served since its mint, and its build link may still name its source's volume. A `cowshed fork` interrupted after its
image was mounted and before its first supervisor start leaves one. A shell or git hook entered there runs the
workspace's Nx against its source's daemon record and task database, with whatever `TMPDIR` and Nx socket directory the
caller inherited. The managed PATH writer, `tooling/direnv/repo-path`, which every managed shell entry and git hook
runs, therefore reads both values from the file alone, never from the caller's environment. In a checkout that carries
`.cowshed/workspace.json`, a file that sets either value not at all or to a relative path is served on the spot: the
writer says so on stderr and runs the start an operator would, `cowshed exec <ws> -- true`, once, from the checkout,
through the installed cowshed (05_gateway.md, "The installed cowshed"), never a checkout's launcher, with neither the
caller's values nor its standard input. That start mounts the checkout, settles its link and serves the file, and the
writer then reads both values from the served file and continues. It refuses only when the start fails, naming the
workspace, its checkout, the start's exit status and its own error, with `cowshed exec <ws> -- true` as the next step;
or when the start succeeds and the file still lacks an entry, naming each; or when `workspace.json` names no workspace
to serve. The managed shell sources the file after the writer, so it enters on the served values. The managed shell's Nx
socket owner takes the served link or refuses (04_sandbox.md). Neither falls back to a temporary or socket directory of
its own.

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
takes the trade deliberately, per workspace (see "Git-worktree workspaces").

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
