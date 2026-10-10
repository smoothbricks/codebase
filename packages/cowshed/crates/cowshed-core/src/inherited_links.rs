//! Restore the meaning of symlinks a new tree inherited from the tree that produced it.
//!
//! A workspace is materialized by cloning one image, so every symlink arrives carrying the
//! exact bytes the source tree recorded. Whether those bytes still mean the same thing
//! depends on one property and nothing else: where the target lands relative to the tree
//! root.
//!
//! A relative target whose climb terminates *inside* the tree keeps its meaning at any
//! depth — it names a path the clone also owns, so it is correct by construction and is
//! never touched here. A relative target that climbs *out* of the tree does not: the number
//! of `..` components was computed against the source tree's position in the filesystem, so
//! in a tree mounted at a different depth the same bytes land somewhere unrelated. It may
//! dangle. Worse, it may resolve — silently, onto a directory that is not what the source
//! meant — and a wrong answer that looks like an answer is the failure this module exists to
//! remove.
//!
//! `bun install` writes exactly such a link for a `link:` dependency, whose target lives in
//! the user's global install root rather than in the repository. Rewriting that link in the
//! source tree is not a fix: it is generated from the dependency declaration and the next
//! install regenerates it. The clone is therefore where the meaning has to be restored, and
//! it is restored from the source tree's own resolution rather than from a guess.
//!
//! The rule is deliberately narrow. Escaping links are rewritten to the absolute path they
//! named in the source tree, which is depth-independent and so survives every later clone.
//! Absolute links into the source tree itself (a hook installer's `.git/hooks` entry, a
//! package linker's link to a directory the checkout owns) named the source checkout's own
//! files: another checkout's, which a sandboxed workspace cannot read and whose edits are not
//! this tree's. They are rewritten to the same path under this tree's root, spelled relative to
//! the link, so they keep naming this tree's copy at whatever depth it is mounted later.
//! In-tree links and absolute links outside the source tree are left alone: rewriting them
//! would spend clone time to change nothing, and the whole point of a copy-on-write workspace
//! is that it is ready in seconds.

use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;

use crate::error::{CowshedError, Result};
use crate::fsio::AnchoredDirectory;
use crate::metadata::{OWNED_LINKS_REVISION, WorkspaceIncarnation, WorkspaceMarker, write_json};
use crate::repository::RepoId;
use crate::storage::WORKSPACE_MARKER_PATH;

/// What a symlink's recorded target means once its tree moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Targeting {
    /// An absolute target names the same path at every tree depth.
    Absolute,
    /// A relative target whose climb terminates inside the tree.
    InTree,
    /// A relative target that climbs above the tree root.
    Escapes,
}

/// Which inherited links a restoration may retarget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreScope {
    /// A new mint's inherited tree: escaping links resolve against the source, and absolute
    /// links into the source are re-spelled relative to this tree.
    Inherited,
    /// The owned-link migration: only absolute links into the donor root are re-spelled. Escaping
    /// links are the current workspace's own and are left exactly as they are.
    OwnedAbsolute,
}

/// Classify a target from the link's parent directory, expressed relative to the tree root.
///
/// Purely lexical, and that is the contract rather than a shortcut: the question is what the
/// *recorded bytes* mean at a given depth. Answering it without touching the filesystem is
/// what keeps this from chasing a link through some other tree's symlinks on the way out,
/// and makes the classification identical whether or not the target happens to exist.
pub fn classify(link_parent: &Path, target: &Path) -> Targeting {
    if target.is_absolute() {
        return Targeting::Absolute;
    }
    let mut depth = link_parent
        .components()
        .filter(|component| matches!(component, Component::Normal(_)))
        .count() as i64;
    for component in target.components() {
        match component {
            Component::ParentDir => {
                depth -= 1;
                // Only a climb that passes *above* the root escapes. An intermediate `..`
                // that a later name re-enters is ordinary path arithmetic, not an escape.
                if depth < 0 {
                    return Targeting::Escapes;
                }
            }
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => return Targeting::Absolute,
        }
    }
    Targeting::InTree
}

/// Collapse `.` and `..` without consulting the filesystem.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// The absolute path a link's recorded target named in the tree that produced it.
///
/// `link_parent` is the link's parent directory relative to the tree root, so the same
/// relative bytes are re-resolved against the source root instead of the clone's root.
pub fn resolve_in_source(source_root: &Path, link_parent: &Path, target: &Path) -> PathBuf {
    normalize(&source_root.join(link_parent).join(target))
}

/// A link whose inherited target means something else in this tree, and the target that
/// restores its source-tree meaning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewrite {
    /// The link itself, relative to the tree root.
    pub at: PathBuf,
    /// The target the tree inherited.
    pub inherited: PathBuf,
    /// What the link points at instead: for an escaping target, the absolute path it named in
    /// the source tree; for an absolute target inside the source tree, the same path under
    /// this tree's root, relative to the link's parent.
    pub restored: PathBuf,
}

/// An escaping link whose source-tree meaning does not exist, so no correct target can be
/// derived from it.
///
/// Named rather than repaired, and never rewritten to a guess: the link is already broken in
/// the source tree, and inventing a target would commit exactly the error — an answer that
/// looks like an answer — that this module removes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// The link itself, relative to the tree root.
    pub at: PathBuf,
    /// The target the tree inherited.
    pub inherited: PathBuf,
    /// The source-tree path that target named, which does not exist.
    pub probed: PathBuf,
}

/// Every link in one tree whose meaning the move changes, split into what can be restored and
/// what cannot.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LinkPlan {
    pub rewrites: Vec<Rewrite>,
    pub refusals: Vec<Refusal>,
    /// Every symlink the walk saw, escaping or not. Reported so that "nothing to do" is
    /// distinguishable from "nothing was looked at" — an empty plan over an empty walk is
    /// not a pass.
    pub symlinks_seen: usize,
}

impl LinkPlan {
    pub fn is_empty(&self) -> bool {
        self.rewrites.is_empty() && self.refusals.is_empty()
    }

    /// One line per refusal, naming the link, what it inherited, and the source path that
    /// did not exist — the three facts needed to fix it without re-deriving them.
    pub fn refusal_report(&self) -> String {
        self.refusals
            .iter()
            .map(|refusal| {
                format!(
                    "{} points outside the workspace at {}, which names {} in the source tree and does not exist",
                    refusal.at.display(),
                    refusal.inherited.display(),
                    refusal.probed.display()
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Concurrent directory readers in one walk.
///
/// The walk runs on a volume attached moments ago, where a directory read waits on the disk
/// image rather than on the CPU. Over a 1M-entry, 138k-directory checkout on a freshly mounted
/// image, one reader took 14.6 s and four 9.7 s. Eight, sixteen and thirty-two readers spent
/// about the same kernel time, and on a busy host their median walks were 6.5 s, 6.2 s and
/// 5.1 s: past that the volume, not the reader count, is the limit. The readers mostly sleep in
/// the kernel, so the count does not follow the host's core count.
const WALKERS: usize = 32;

/// What a symlink whose meaning moved contributes to a plan.
enum Found {
    Rewrite(Rewrite),
    Refusal(Refusal),
}

impl Found {
    fn at(&self) -> &Path {
        match self {
            Self::Rewrite(rewrite) => &rewrite.at,
            Self::Refusal(refusal) => &refusal.at,
        }
    }
}

/// Walk `tree_root` and decide what each symlink whose meaning moved should become.
///
/// The walk never follows links, so it cannot leave the tree it was handed or cycle through
/// one that points back into itself. Directories are read concurrently; the plan is sorted by
/// link path, so it does not depend on which reader reached a link first.
pub fn plan(tree_root: &Path, source_root: &Path, scope: RestoreScope) -> Result<LinkPlan> {
    let (symlinks_seen, mut found) = walk_symlinks(tree_root, |relative| {
        judge(tree_root, source_root, relative, scope)
    });
    found.sort_unstable_by(|left, right| left.at().cmp(right.at()));
    let mut plan = LinkPlan {
        symlinks_seen,
        ..LinkPlan::default()
    };
    for verdict in found {
        match verdict {
            Found::Rewrite(rewrite) => plan.rewrites.push(rewrite),
            Found::Refusal(refusal) => plan.refusals.push(refusal),
        }
    }
    Ok(plan)
}

/// Classify one symlink, `relative` to the tree root. An escaping target is resolved against
/// the source tree; an absolute target inside the source tree is re-spelled relative to the
/// same path in this tree. `None` is a link that keeps its meaning at any depth.
fn judge(
    tree_root: &Path,
    source_root: &Path,
    relative: PathBuf,
    scope: RestoreScope,
) -> Option<Found> {
    // A link whose target cannot be read is not this module's business: it is reported by
    // whatever reads it, and guessing at a replacement would be worse than leaving it exactly
    // as the source tree had it.
    let target = fs::read_link(tree_root.join(&relative)).ok()?;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    match classify(parent, &target) {
        Targeting::InTree => None,
        Targeting::Absolute => {
            // Lexical, like the classification: whether the source path exists does not
            // change what the bytes named, and this tree holds the same paths the source does.
            let inside = normalize(&target)
                .strip_prefix(source_root)
                .ok()?
                .to_owned();
            Some(Found::Rewrite(Rewrite {
                restored: relative_from(parent, &inside),
                at: relative,
                inherited: target,
            }))
        }
        Targeting::Escapes if scope == RestoreScope::OwnedAbsolute => None,
        Targeting::Escapes => {
            let restored = resolve_in_source(source_root, parent, &target);
            // `symlink_metadata`, not `exists`: a source target that is itself a symlink counts
            // as present, and following it here would resolve someone else's link for them.
            Some(if fs::symlink_metadata(&restored).is_ok() {
                Found::Rewrite(Rewrite {
                    at: relative,
                    inherited: target,
                    restored,
                })
            } else {
                Found::Refusal(Refusal {
                    at: relative,
                    inherited: target,
                    probed: restored,
                })
            })
        }
    }
}

/// `inside`, a path relative to the tree root, spelled from `link_parent` (also relative to
/// the tree root): one `..` per directory the parent is below the root, then `inside`.
fn relative_from(link_parent: &Path, inside: &Path) -> PathBuf {
    let mut relative: PathBuf = link_parent
        .components()
        .filter(|component| matches!(component, Component::Normal(_)))
        .map(|_| Component::ParentDir)
        .collect();
    if !inside.as_os_str().is_empty() {
        relative.push(inside);
    }
    if relative.as_os_str().is_empty() {
        relative.push(Component::CurDir);
    }
    relative
}

/// The directories one walk has yet to read, shared by its readers.
struct Walk {
    queue: Mutex<Pending>,
    changed: Condvar,
}

/// Directories not yet taken, and how many readers hold one they are still reading.
struct Pending {
    directories: Vec<PathBuf>,
    reading: usize,
}

/// One directory a reader holds. The walk cannot end while any claim is live, because the
/// directory being read may add more.
struct Claim<'walk> {
    walk: &'walk Walk,
    directory: PathBuf,
    children: Vec<PathBuf>,
}

impl Walk {
    fn pending(&self) -> MutexGuard<'_, Pending> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take a directory to read, waiting while another reader may still add one. `None` means
    /// the walk is over: nothing is pending and no reader holds a directory, the only state in
    /// which no further directory can appear.
    fn take(&self) -> Option<Claim<'_>> {
        let mut pending = self.pending();
        loop {
            if let Some(directory) = pending.directories.pop() {
                pending.reading += 1;
                return Some(Claim {
                    walk: self,
                    directory,
                    children: Vec::new(),
                });
            }
            if pending.reading == 0 {
                return None;
            }
            pending = self
                .changed
                .wait(pending)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

impl Drop for Claim<'_> {
    /// Hand back the children and release the directory. Doing it on drop is what lets a reader
    /// that panics still end the walk: a claim that never released would leave every other
    /// reader waiting for children that cannot arrive, and the scope joining them would hang.
    fn drop(&mut self) {
        let mut pending = self.walk.pending();
        pending.reading -= 1;
        let wake = !self.children.is_empty() || pending.reading == 0;
        pending.directories.append(&mut self.children);
        drop(pending);
        if wake {
            self.walk.changed.notify_all();
        }
    }
}

/// Visit every symlink under `root` with `visit`, never following one; answer how many
/// symlinks there were and everything `visit` kept.
///
/// Only directories and symlinks get a path built: regular files, the bulk of any tree, cost
/// one directory entry and nothing else. An unreadable directory or entry is skipped rather
/// than failing the walk, the same answer a single sequential reader gives.
fn walk_symlinks<T, F>(root: &Path, visit: F) -> (usize, Vec<T>)
where
    T: Send,
    F: Fn(PathBuf) -> Option<T> + Sync,
{
    let walk = Walk {
        queue: Mutex::new(Pending {
            directories: vec![PathBuf::new()],
            reading: 0,
        }),
        changed: Condvar::new(),
    };
    let read = || {
        let mut seen = 0_usize;
        let mut kept = Vec::new();
        while let Some(mut claim) = walk.take() {
            let Ok(entries) = fs::read_dir(root.join(&claim.directory)) else {
                continue;
            };
            for entry in entries.flatten() {
                let Ok(kind) = entry.file_type() else {
                    continue;
                };
                if kind.is_symlink() {
                    seen += 1;
                    kept.extend(visit(claim.directory.join(entry.file_name())));
                } else if kind.is_dir() {
                    claim.children.push(claim.directory.join(entry.file_name()));
                }
            }
        }
        (seen, kept)
    };
    thread::scope(|scope| {
        // Collected before any join: joining as they spawn would run the readers one by one.
        let readers: Vec<_> = (0..WALKERS).map(|_| scope.spawn(read)).collect();
        readers
            .into_iter()
            .fold((0, Vec::new()), |(seen, mut kept), reader| {
                let (reader_seen, reader_kept) = reader
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                kept.extend(reader_kept);
                (seen + reader_seen, kept)
            })
    })
}

/// Replace each planned link with its restored target. Links are grouped by their held parent:
/// each parent is opened once, every link in it is repointed through that handle, and a parent
/// that changed is synced once before `apply` returns. A marker written after `apply` therefore
/// never names a link the disk does not yet hold.
pub fn apply(tree_root: &Path, plan: &LinkPlan) -> Result<()> {
    let mut rewrites: Vec<&Rewrite> = plan.rewrites.iter().collect();
    rewrites.sort_by(|left, right| left.at.parent().cmp(&right.at.parent()));
    for group in rewrites.chunk_by(|left, right| left.at.parent() == right.at.parent()) {
        let first = tree_root.join(&group[0].at);
        let parent = first.parent().expect("a planned link has a parent");
        let directory = AnchoredDirectory::open(parent).map_err(|source| {
            CowshedError::integrity(
                format!(
                    "could not open {} to repoint its link: {source}",
                    parent.display()
                ),
                "check the workspace mount is writable, then retry",
            )
        })?;
        let mut changed = false;
        for rewrite in group {
            let name = rewrite.at.file_name().expect("a planned link has a name");
            let name = CString::new(name.as_bytes()).map_err(|_| {
                CowshedError::integrity(
                    format!(
                        "link name at {} contains NUL",
                        tree_root.join(&rewrite.at).display()
                    ),
                    "repair the workspace link name",
                )
            })?;
            changed |= directory
                .exact_symlink(&name, &rewrite.restored)
                .map_err(|source| {
                    CowshedError::integrity(
                        format!(
                            "could not point {} at {}: {source}",
                            tree_root.join(&rewrite.at).display(),
                            rewrite.restored.display()
                        ),
                        "check the workspace mount is writable, then retry",
                    )
                })?;
        }
        if changed {
            directory.sync().map_err(|source| {
                CowshedError::integrity(
                    format!(
                        "could not sync {} after repointing its links: {source}",
                        parent.display()
                    ),
                    "check the workspace mount is writable, then retry",
                )
            })?;
        }
    }
    Ok(())
}

/// Plan and apply in one step, returning the plan so the caller can report refusals.
///
/// A plan with any refusal applies nothing. The tree then still holds exactly the bytes it
/// inherited, so a retry after the source is repaired judges every link afresh against the
/// tree that produced it, instead of inheriting half of a pass that already failed.
pub fn restore(tree_root: &Path, source_root: &Path, scope: RestoreScope) -> Result<LinkPlan> {
    let plan = plan(tree_root, source_root, scope)?;
    if plan.refusals.is_empty() {
        apply(tree_root, &plan)?;
    }
    Ok(plan)
}

/// The one-time owned-link migration of a workspace whose marker predates
/// [`OWNED_LINKS_REVISION`]: absolute links into `donor` are re-spelled relative to this tree.
///
/// The protected marker must name this repository, incarnation and donor before anything is
/// walked or written. Returns `None` without a walk when the marker is already at the current
/// revision.
pub fn migrate_owned_links(
    root: &Path,
    donor: &Path,
    expected_repo: &RepoId,
    expected_incarnation: &WorkspaceIncarnation,
) -> Result<Option<LinkPlan>> {
    let marker_path = root.join(WORKSPACE_MARKER_PATH);
    let mut marker = WorkspaceMarker::read_from(&marker_path).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot read workspace marker {}: {error}",
                marker_path.display()
            ),
            "repair the workspace marker, then retry",
        )
    })?;
    if marker.repo_id != *expected_repo || marker.workspace_incarnation != *expected_incarnation {
        return Err(CowshedError::integrity(
            format!(
                "workspace marker {} does not name this repository and incarnation",
                marker_path.display()
            ),
            "refuse the migration; the marker and this workspace disagree",
        ));
    }
    if marker.owned_links_revision >= OWNED_LINKS_REVISION {
        return Ok(None);
    }
    if marker.project_root != donor {
        return Err(CowshedError::integrity(
            format!(
                "workspace marker {} names a different donor than {}",
                marker_path.display(),
                donor.display()
            ),
            "refuse the migration; the marker and the donor disagree",
        ));
    }
    // A legacy tree is applied one held parent at a time. A crash between parents leaves some of
    // them done, and the next run plans only the links still naming the donor: a rewritten link is
    // relative, so it is never planned again.
    let plan = plan(root, donor, RestoreScope::OwnedAbsolute)?;
    apply(root, &plan)?;
    marker.owned_links_revision = OWNED_LINKS_REVISION;
    write_json(&marker_path, &marker).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot record the owned-link revision in {}: {error}",
                marker_path.display()
            ),
            "repair the workspace marker, then retry",
        )
    })?;
    Ok(Some(plan))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsio::{Syncs, take_syncs};
    use std::os::unix::fs::symlink;

    /// A scratch tree removed when dropped, so a failed assertion — or the deliberate panic one
    /// test provokes — unwinds through the cleanup a passing test runs.
    struct TempTree(PathBuf);

    impl std::ops::Deref for TempTree {
        type Target = Path;

        fn deref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0) {
                eprintln!(
                    "inherited-links test tree {} was not removed: {error}",
                    self.0.display()
                );
            }
        }
    }

    fn temp_tree(label: &str) -> TempTree {
        let root = std::env::temp_dir().join(format!(
            "cowshed-inherited-links-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create temp tree");
        TempTree(root)
    }

    #[test]
    fn classification_turns_on_where_the_climb_lands_not_on_how_it_is_spelled() {
        // Deep enough that the climb stays inside: the link keeps its meaning anywhere.
        assert_eq!(
            classify(
                Path::new("packages/app/node_modules"),
                Path::new("../../../vendor/lib")
            ),
            Targeting::InTree
        );
        // One more level of climb leaves the tree.
        assert_eq!(
            classify(
                Path::new("packages/app/node_modules"),
                Path::new("../../../../vendor/lib")
            ),
            Targeting::Escapes
        );
        // A `..` that a later name re-enters is arithmetic, not an escape.
        assert_eq!(
            classify(Path::new("a/b"), Path::new("../../c/../d")),
            Targeting::InTree
        );
        // Absolute targets are depth-independent already.
        assert_eq!(
            classify(Path::new("a/b"), Path::new("/opt/lib")),
            Targeting::Absolute
        );
        // A link at the tree root has no depth to spend.
        assert_eq!(
            classify(Path::new(""), Path::new("../sibling")),
            Targeting::Escapes
        );
        assert_eq!(
            classify(Path::new(""), Path::new("./inside")),
            Targeting::InTree
        );
    }

    #[test]
    fn an_escaping_link_is_restored_to_what_it_named_in_the_source_tree() {
        // The shape `bun install` leaves for a `link:` dependency: a climb out of the tree
        // into a directory beside it, valid at exactly the source tree's depth.
        let base = temp_tree("restore");
        let global = base.join("global/node_modules/pkg");
        fs::create_dir_all(&global).expect("global target");
        let source = base.join("source");
        let nested = source.join("packages/app/node_modules");
        fs::create_dir_all(&nested).expect("source tree");
        symlink("../../../../global/node_modules/pkg", nested.join("pkg")).expect("source link");

        // The clone sits one level deeper, so the same recorded bytes miss.
        let clone = base.join("deeper/clone");
        let clone_nested = clone.join("packages/app/node_modules");
        fs::create_dir_all(&clone_nested).expect("clone tree");
        symlink(
            "../../../../global/node_modules/pkg",
            clone_nested.join("pkg"),
        )
        .expect("clone link");
        assert!(
            !clone_nested.join("pkg").exists(),
            "the inherited link must be broken in the clone, or this test proves nothing"
        );

        let plan = restore(&clone, &source, RestoreScope::Inherited).expect("restore");
        assert_eq!(plan.rewrites.len(), 1);
        assert!(plan.refusals.is_empty());
        assert_eq!(
            fs::read_link(clone_nested.join("pkg")).expect("link"),
            global
        );
        assert!(
            clone_nested.join("pkg").exists(),
            "restored link must resolve"
        );
    }

    #[test]
    fn a_link_that_resolves_only_by_accident_is_repointed_at_the_source_meaning() {
        // The dangerous case, and the reason this cannot be "repair only what dangles": the
        // inherited bytes DO resolve in the clone, onto a directory that is not what the
        // source tree meant. Nothing reports an error; the wrong package is simply used.
        let base = temp_tree("accident");
        let intended = base.join("source-side/node_modules/pkg");
        fs::create_dir_all(intended.join("real")).expect("intended target");
        let source = base.join("source-side/tree");
        let source_nested = source.join("packages/app");
        fs::create_dir_all(&source_nested).expect("source tree");
        symlink("../../../node_modules/pkg", source_nested.join("pkg")).expect("source link");

        let clone = base.join("clone-side/tree");
        let clone_nested = clone.join("packages/app");
        fs::create_dir_all(&clone_nested).expect("clone tree");
        let decoy = base.join("clone-side/node_modules/pkg");
        fs::create_dir_all(decoy.join("decoy")).expect("decoy");
        symlink("../../../node_modules/pkg", clone_nested.join("pkg")).expect("clone link");
        assert!(
            clone_nested.join("pkg").join("decoy").exists(),
            "the inherited link must resolve onto the decoy, or this test proves nothing"
        );

        let plan = restore(&clone, &source, RestoreScope::Inherited).expect("restore");
        assert_eq!(
            plan.rewrites.len(),
            1,
            "an accidental resolution is still an escape"
        );
        assert_eq!(
            fs::read_link(clone_nested.join("pkg")).expect("link"),
            intended
        );
        assert!(clone_nested.join("pkg").join("real").exists());
    }

    #[test]
    fn in_tree_links_and_absolute_links_outside_the_source_are_left_byte_for_byte_alone() {
        let base = temp_tree("untouched");
        let source = base.join("source");
        let clone = base.join("clone");
        for root in [&source, &clone] {
            fs::create_dir_all(root.join("vendor/lib")).expect("vendor");
            fs::create_dir_all(root.join("packages/app")).expect("app");
            symlink("../../vendor/lib", root.join("packages/app/lib")).expect("in-tree link");
            symlink("/opt/lib", root.join("packages/app/abs")).expect("absolute link");
        }

        let plan = restore(&clone, &source, RestoreScope::Inherited).expect("restore");
        assert!(plan.is_empty(), "no escaping links, so nothing to rewrite");
        assert_eq!(plan.symlinks_seen, 2, "both links must have been examined");
        assert_eq!(
            fs::read_link(clone.join("packages/app/lib")).expect("link"),
            Path::new("../../vendor/lib"),
            "an in-tree link is correct by construction and must keep its relative form"
        );
        assert_eq!(
            fs::read_link(clone.join("packages/app/abs")).expect("link"),
            Path::new("/opt/lib")
        );
    }

    #[test]
    fn an_absolute_link_into_the_source_tree_is_respelled_relative_to_the_clones_own_copy() {
        // The shapes a host leaves in main: a hook installer's absolute `.git/hooks` entry and
        // a package linker's absolute link to a directory main owns. Inherited as they are,
        // both name main's files from inside the clone.
        let base = temp_tree("absolute-into-source");
        let source = base.join("source");
        let clone = base.join("deeper/clone");
        for root in [&source, &clone] {
            fs::create_dir_all(root.join(".git/hooks")).expect("hooks");
            fs::create_dir_all(root.join("tooling/git-hooks")).expect("hook sources");
            fs::write(root.join("tooling/git-hooks/pre-commit.sh"), "").expect("hook source");
            fs::create_dir_all(root.join(".cache/patched-nx/package")).expect("package");
            fs::create_dir_all(root.join("node_modules")).expect("node_modules");
        }
        symlink(
            source.join("tooling/git-hooks/pre-commit.sh"),
            clone.join(".git/hooks/pre-commit"),
        )
        .expect("hook link");
        symlink(
            source.join(".cache/patched-nx/package"),
            clone.join("node_modules/nx"),
        )
        .expect("package link");
        symlink(&source, clone.join("root")).expect("root link");
        symlink("/opt/lib", clone.join("node_modules/outside")).expect("outside link");

        let plan = restore(&clone, &source, RestoreScope::Inherited).expect("restore");
        assert!(plan.refusals.is_empty());
        assert_eq!(
            plan.rewrites.len(),
            3,
            "every absolute link into the source, and only those"
        );
        assert_eq!(
            fs::read_link(clone.join(".git/hooks/pre-commit")).expect("hook"),
            Path::new("../../tooling/git-hooks/pre-commit.sh")
        );
        assert_eq!(
            fs::read_link(clone.join("node_modules/nx")).expect("package"),
            Path::new("../.cache/patched-nx/package")
        );
        assert_eq!(
            fs::read_link(clone.join("root")).expect("root"),
            Path::new(".")
        );
        assert_eq!(
            clone
                .join("node_modules/nx")
                .canonicalize()
                .expect("package resolves"),
            clone
                .join(".cache/patched-nx/package")
                .canonicalize()
                .expect("clone package"),
            "the link names the clone's own copy, never the source's"
        );
        assert_eq!(
            fs::read_link(clone.join("node_modules/outside")).expect("outside"),
            Path::new("/opt/lib"),
            "an absolute link outside the source tree is not the source's to re-spell"
        );
    }

    #[test]
    fn an_escaping_link_missing_from_the_source_is_named_never_guessed() {
        let base = temp_tree("refusal");
        fs::create_dir_all(base.join("present/pkg")).expect("restorable target");
        let source = base.join("source");
        fs::create_dir_all(source.join("packages/app")).expect("source tree");
        let clone = base.join("clone");
        let clone_nested = clone.join("packages/app");
        fs::create_dir_all(&clone_nested).expect("clone tree");
        symlink("../../../gone/pkg", clone_nested.join("pkg")).expect("clone link");
        symlink("../../../present/pkg", clone_nested.join("present")).expect("restorable link");

        let plan = restore(&clone, &source, RestoreScope::Inherited).expect("restore");
        assert_eq!(
            plan.rewrites.len(),
            1,
            "the restorable link is still planned"
        );
        assert_eq!(plan.refusals.len(), 1);
        let refusal = &plan.refusals[0];
        assert_eq!(refusal.at, Path::new("packages/app/pkg"));
        assert_eq!(refusal.probed, base.join("gone/pkg"));
        assert_eq!(
            fs::read_link(clone_nested.join("pkg")).expect("link"),
            Path::new("../../../gone/pkg"),
            "a refused link is left exactly as inherited"
        );
        assert_eq!(
            fs::read_link(clone_nested.join("present")).expect("link"),
            Path::new("../../../present/pkg"),
            "a refused plan writes nothing, so a retry judges inherited bytes only"
        );
        let report = plan.refusal_report();
        assert!(
            report.contains("packages/app/pkg"),
            "the report must name the link: {report}"
        );
        assert!(
            report.contains("gone/pkg"),
            "the report must name what was probed: {report}"
        );
    }

    #[test]
    fn a_wide_deep_tree_yields_every_link_exactly_once_in_path_order() {
        // Enough directories that every reader takes work, at mixed depths so the readers
        // finish in no particular order: the plan must still be complete and sorted.
        let base = temp_tree("wide");
        let source = base.join("source");
        let clone = base.join("clone");
        fs::create_dir_all(base.join("outside")).expect("outside target");
        fs::create_dir_all(&source).expect("source");
        let mut expected = Vec::new();
        for branch in 0..24 {
            let mut relative = PathBuf::from(format!("b{branch:02}"));
            for depth in 0..(branch % 6 + 1) {
                relative.push(format!("d{depth}"));
                let directory = clone.join(&relative);
                fs::create_dir_all(&directory).expect("clone dir");
                fs::create_dir_all(source.join(&relative)).expect("source dir");
                let climb = "../".repeat(relative.components().count() + 1);
                symlink(format!("{climb}outside"), directory.join("out")).expect("escaping");
                symlink("/opt/lib", directory.join("abs")).expect("absolute");
                expected.push(relative.join("out"));
            }
        }
        expected.sort();

        let plan = plan(&clone, &source, RestoreScope::Inherited).expect("plan");
        let found: Vec<_> = plan
            .rewrites
            .iter()
            .map(|rewrite| rewrite.at.clone())
            .collect();
        assert_eq!(found, expected, "every escaping link once, ordered by path");
        assert_eq!(plan.symlinks_seen, expected.len() * 2);
        assert!(plan.refusals.is_empty());
        assert!(
            plan.rewrites
                .iter()
                .all(|rewrite| rewrite.restored == base.join("outside")),
            "each climb names the directory beside the source tree"
        );
    }

    #[test]
    fn a_reader_that_panics_ends_the_walk_instead_of_stranding_the_others() {
        // Without the claim released on unwind, the panicking reader's directory would stay
        // counted as being read, every other reader would wait for its children forever, and
        // this test would hang instead of failing.
        let base = temp_tree("panic");
        for branch in 0..(WALKERS * 4) {
            let directory = base.join(format!("b{branch:02}"));
            fs::create_dir_all(directory.join("inner")).expect("branch");
            symlink("inner", directory.join("link")).expect("link");
        }
        let poisoned = Path::new("b07/link");

        let outcome = std::panic::catch_unwind(|| {
            walk_symlinks(&base, |relative| {
                assert_ne!(relative, poisoned, "reader fails on one link");
                None::<()>
            })
        });

        assert!(outcome.is_err(), "the reader's panic reaches the caller");
    }

    #[test]
    fn the_walk_reports_what_it_examined_so_an_empty_plan_is_not_a_blind_pass() {
        let base = temp_tree("nonvacuous");
        let source = base.join("source");
        let clone = base.join("clone");
        fs::create_dir_all(&source).expect("source");
        fs::create_dir_all(clone.join("empty/nested")).expect("clone");

        let plan = restore(&clone, &source, RestoreScope::Inherited).expect("restore");
        assert!(plan.is_empty());
        assert_eq!(
            plan.symlinks_seen, 0,
            "a tree with no symlinks must report zero examined, not silently succeed"
        );
    }

    #[test]
    fn owned_link_migration_retargets_absolute_donor_links_once() {
        let donor = temp_tree("owned-donor");
        let tree = temp_tree("owned-tree");
        fs::create_dir_all(donor.join(".cache/owned")).expect("donor product");
        fs::create_dir_all(donor.join("node_modules")).expect("donor modules");
        fs::create_dir_all(tree.join(".cowshed")).expect("marker parent");
        fs::create_dir_all(tree.join("node_modules")).expect("tree modules");
        symlink(donor.join(".cache/owned"), tree.join("node_modules/owned"))
            .expect("inherited owned link");
        symlink("../../../external/pkg", tree.join("node_modules/sibling"))
            .expect("job-made sibling");
        let marker: WorkspaceMarker = serde_json::from_value(serde_json::json!({
            "version": 1,
            "repoId": "acme/widget",
            "projectRoot": donor.display().to_string(),
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "baseCommit": "8f31c2d",
            "createdAt": "2026-07-11T12:00:00Z",
            "forkedFrom": null,
            "createdTrace": "4bf92f"
        }))
        .expect("legacy marker");
        write_json(&tree.join(WORKSPACE_MARKER_PATH), &marker).expect("write legacy marker");
        let repo = RepoId::parse("acme/widget").expect("repo");
        let incarnation =
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("incarnation");
        let migrated = migrate_owned_links(&tree, &donor, &repo, &incarnation)
            .expect("migrate")
            .expect("first migration runs");
        assert_eq!(migrated.rewrites.len(), 1);
        assert_eq!(
            fs::read_link(tree.join("node_modules/owned")).expect("owned link"),
            Path::new("../.cache/owned")
        );
        assert_eq!(
            fs::read_link(tree.join("node_modules/sibling")).expect("sibling link"),
            Path::new("../../../external/pkg")
        );
        assert_eq!(
            WorkspaceMarker::read_from(&tree.join(WORKSPACE_MARKER_PATH))
                .expect("marker")
                .owned_links_revision,
            OWNED_LINKS_REVISION
        );
        symlink(donor.join(".cache/owned"), tree.join("node_modules/late")).expect("late link");
        assert!(
            migrate_owned_links(&tree, &donor, &repo, &incarnation)
                .expect("second migrate")
                .is_none()
        );
        assert_eq!(
            fs::read_link(tree.join("node_modules/late")).expect("late link"),
            donor.join(".cache/owned")
        );
    }

    #[test]
    fn a_new_mint_at_the_current_revision_skips_the_walk_and_leaves_late_donor_links_alone() {
        let donor = temp_tree("owned-current-donor");
        let tree = temp_tree("owned-current-tree");
        fs::create_dir_all(donor.join(".cache/owned")).expect("donor product");
        fs::create_dir_all(tree.join(".cowshed")).expect("marker parent");
        fs::create_dir_all(tree.join("node_modules")).expect("tree modules");
        let marker: WorkspaceMarker = serde_json::from_value(serde_json::json!({
            "version": 1,
            "repoId": "acme/widget",
            "projectRoot": donor.display().to_string(),
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "baseCommit": "8f31c2d",
            "createdAt": "2026-07-11T12:00:00Z",
            "forkedFrom": null,
            "createdTrace": "4bf92f",
            "ownedLinksRevision": OWNED_LINKS_REVISION
        }))
        .expect("current marker");
        write_json(&tree.join(WORKSPACE_MARKER_PATH), &marker).expect("write current marker");
        symlink(donor.join(".cache/owned"), tree.join("node_modules/late"))
            .expect("late donor link");
        let repo = RepoId::parse("acme/widget").expect("repo");
        let incarnation =
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("incarnation");
        assert!(
            migrate_owned_links(&tree, &donor, &repo, &incarnation)
                .expect("migrate at current revision")
                .is_none()
        );
        assert_eq!(
            fs::read_link(tree.join("node_modules/late")).expect("late link"),
            donor.join(".cache/owned")
        );
    }

    #[test]
    fn an_interrupted_owned_migration_converges_on_the_next_run_without_absence() {
        let donor = temp_tree("owned-prefix-donor");
        let tree = temp_tree("owned-prefix-tree");
        fs::create_dir_all(donor.join(".cache/a")).expect("donor a");
        fs::create_dir_all(donor.join(".cache/b")).expect("donor b");
        fs::create_dir_all(tree.join(".cowshed")).expect("marker parent");
        fs::create_dir_all(tree.join("node_modules")).expect("tree modules");
        symlink(donor.join(".cache/a"), tree.join("node_modules/a")).expect("a");
        symlink(donor.join(".cache/b"), tree.join("node_modules/b")).expect("b");
        let marker: WorkspaceMarker = serde_json::from_value(serde_json::json!({
            "version": 1,
            "repoId": "acme/widget",
            "projectRoot": donor.display().to_string(),
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "baseCommit": "8f31c2d",
            "createdAt": "2026-07-11T12:00:00Z",
            "forkedFrom": null,
            "createdTrace": "4bf92f"
        }))
        .expect("legacy marker");
        write_json(&tree.join(WORKSPACE_MARKER_PATH), &marker).expect("write legacy marker");
        let repo = RepoId::parse("acme/widget").expect("repo");
        let incarnation =
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("incarnation");
        // A crash after the first link and before the marker: only that prefix is applied.
        let mut partial = plan(&tree, &donor, RestoreScope::OwnedAbsolute).expect("plan");
        assert_eq!(partial.rewrites.len(), 2);
        partial.rewrites.truncate(1);
        apply(&tree, &partial).expect("first link only");
        assert_eq!(
            WorkspaceMarker::read_from(&tree.join(WORKSPACE_MARKER_PATH))
                .expect("marker")
                .owned_links_revision,
            0,
            "the marker still names the legacy revision"
        );
        let rest = migrate_owned_links(&tree, &donor, &repo, &incarnation)
            .expect("converge")
            .expect("the next run runs");
        assert_eq!(rest.rewrites.len(), 1, "only the remaining link is planned");
        for name in ["a", "b"] {
            assert_eq!(
                fs::read_link(tree.join("node_modules").join(name)).expect("link never absent"),
                Path::new(&format!("../.cache/{name}"))
            );
        }
        assert_eq!(
            WorkspaceMarker::read_from(&tree.join(WORKSPACE_MARKER_PATH))
                .expect("marker")
                .owned_links_revision,
            OWNED_LINKS_REVISION
        );
    }

    /// Each parent holding a changed link is synced once, after its last repoint and before `apply`
    /// returns. A parent whose links already name their targets is not synced at all.
    #[test]
    fn apply_syncs_each_touched_parent_once_before_returning() {
        let donor = temp_tree("apply-sync-donor");
        let tree = temp_tree("apply-sync-tree");
        fs::create_dir_all(tree.join("node_modules/@scope")).expect("tree modules");
        let mut rewrites = Vec::new();
        for at in ["node_modules/a", "node_modules/b", "node_modules/@scope/c"] {
            symlink(donor.join("old"), tree.join(at)).expect("inherited link");
            rewrites.push(Rewrite {
                at: PathBuf::from(at),
                inherited: donor.join("old"),
                restored: donor.join("new"),
            });
        }
        let plan = LinkPlan {
            rewrites,
            refusals: Vec::new(),
            symlinks_seen: 3,
        };
        take_syncs();
        apply(&tree, &plan).expect("apply");
        assert_eq!(
            take_syncs(),
            Syncs { fsync: 0, full: 2 },
            "one sync for node_modules and one for node_modules/@scope"
        );
        apply(&tree, &plan).expect("apply again");
        assert_eq!(
            take_syncs(),
            Syncs::default(),
            "links that already name their targets are kept without a sync"
        );
    }
}
