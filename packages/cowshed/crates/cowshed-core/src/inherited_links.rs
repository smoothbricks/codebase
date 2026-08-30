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
//! In-tree links and already-absolute links are left alone: rewriting them would spend clone
//! time to change nothing, and the whole point of a copy-on-write workspace is that it is
//! ready in seconds.

use std::fs;
use std::path::{Component, Path, PathBuf};

use walkdir::WalkDir;

use crate::error::{CowshedError, Result};

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

/// An escaping link and the absolute target that restores its source-tree meaning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewrite {
    /// The link itself, relative to the tree root.
    pub at: PathBuf,
    /// The target the tree inherited.
    pub inherited: PathBuf,
    /// The absolute path that target named in the source tree.
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

/// Every escaping link in one tree, split into what can be restored and what cannot.
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

/// Walk `tree_root` and decide what each escaping symlink should become.
///
/// The walk never follows links, so it cannot leave the tree it was handed or cycle through
/// one that points back into itself.
pub fn plan(tree_root: &Path, source_root: &Path) -> Result<LinkPlan> {
    let mut plan = LinkPlan::default();
    for entry in WalkDir::new(tree_root)
        .follow_links(false)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if !entry.file_type().is_symlink() {
            continue;
        }
        plan.symlinks_seen += 1;
        let path = entry.path();
        let Ok(relative) = path.strip_prefix(tree_root) else {
            continue;
        };
        // A link whose target cannot be read is not this module's business: it is reported
        // by whatever reads it, and guessing at a replacement would be worse than leaving
        // it exactly as the source tree had it.
        let Ok(target) = fs::read_link(path) else {
            continue;
        };
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));
        if classify(parent, &target) != Targeting::Escapes {
            continue;
        }
        let restored = resolve_in_source(source_root, parent, &target);
        // `symlink_metadata`, not `exists`: a source target that is itself a symlink counts
        // as present, and following it here would resolve someone else's link for them.
        if fs::symlink_metadata(&restored).is_ok() {
            plan.rewrites.push(Rewrite {
                at: relative.to_path_buf(),
                inherited: target,
                restored,
            });
        } else {
            plan.refusals.push(Refusal {
                at: relative.to_path_buf(),
                inherited: target,
                probed: restored,
            });
        }
    }
    Ok(plan)
}

/// Replace each planned link with its absolute source-tree target.
pub fn apply(tree_root: &Path, plan: &LinkPlan) -> Result<()> {
    for rewrite in &plan.rewrites {
        let path = tree_root.join(&rewrite.at);
        fs::remove_file(&path).map_err(|source| {
            CowshedError::integrity(
                format!(
                    "could not replace the inherited link {}: {source}",
                    path.display()
                ),
                "check the workspace mount is writable, then retry",
            )
        })?;
        std::os::unix::fs::symlink(&rewrite.restored, &path).map_err(|source| {
            CowshedError::integrity(
                format!(
                    "could not point {} at {}: {source}",
                    path.display(),
                    rewrite.restored.display()
                ),
                "check the workspace mount is writable, then retry",
            )
        })?;
    }
    Ok(())
}

/// Plan and apply in one step, returning what was done so the caller can report refusals.
pub fn restore(tree_root: &Path, source_root: &Path) -> Result<LinkPlan> {
    let plan = plan(tree_root, source_root)?;
    apply(tree_root, &plan)?;
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn temp_tree(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-inherited-links-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create temp tree");
        root
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

        let plan = restore(&clone, &source).expect("restore");
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

        let plan = restore(&clone, &source).expect("restore");
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
    fn in_tree_and_absolute_links_are_left_byte_for_byte_alone() {
        let base = temp_tree("untouched");
        let source = base.join("source");
        let clone = base.join("clone");
        for root in [&source, &clone] {
            fs::create_dir_all(root.join("vendor/lib")).expect("vendor");
            fs::create_dir_all(root.join("packages/app")).expect("app");
            symlink("../../vendor/lib", root.join("packages/app/lib")).expect("in-tree link");
            symlink("/opt/lib", root.join("packages/app/abs")).expect("absolute link");
        }

        let plan = restore(&clone, &source).expect("restore");
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
    fn an_escaping_link_missing_from_the_source_is_named_never_guessed() {
        let base = temp_tree("refusal");
        let source = base.join("source");
        fs::create_dir_all(source.join("packages/app")).expect("source tree");
        let clone = base.join("clone");
        let clone_nested = clone.join("packages/app");
        fs::create_dir_all(&clone_nested).expect("clone tree");
        symlink("../../../gone/pkg", clone_nested.join("pkg")).expect("clone link");

        let plan = restore(&clone, &source).expect("restore");
        assert!(
            plan.rewrites.is_empty(),
            "nothing correct can be derived, so nothing is written"
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
    fn the_walk_reports_what_it_examined_so_an_empty_plan_is_not_a_blind_pass() {
        let base = temp_tree("nonvacuous");
        let source = base.join("source");
        let clone = base.join("clone");
        fs::create_dir_all(&source).expect("source");
        fs::create_dir_all(clone.join("empty/nested")).expect("clone");

        let plan = restore(&clone, &source).expect("restore");
        assert!(plan.is_empty());
        assert_eq!(
            plan.symlinks_seen, 0,
            "a tree with no symlinks must report zero examined, not silently succeed"
        );
    }
}
