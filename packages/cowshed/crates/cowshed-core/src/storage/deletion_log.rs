//! Controller-initiated destruction evidence.
//!
//! Every destructive filesystem mutation the controller performs on store artifacts is
//! appended here as one JSON object per line, so a later reader (doctor, a human) can tell
//! "the controller removed this" apart from "something outside the controller removed this".
//!
//! Design notes, in order of the mistakes they prevent:
//!
//! * The log is **evidence, not a lock**: every write path is best-effort and swallows all
//!   I/O errors. A read-only or corrupt log must never fail the destructive op it records —
//!   the op already happened; failing afterwards would only lie about that.
//! * One line per actual unlink, with the exact artifact path. A reclaim that removes an
//!   image, its companion, and its sidecar writes three true lines rather than one summary.
//!   Lookup returns the newest match, so re-deletion never hides the first fact.
//! * Timestamps are zero-padded UTC (`YYYY-MM-DDTHH:MM:SSZ`) so lexicographic order is
//!   chronological order and "newest" needs no date parser.
//! * The project root is derived lexically from the image path at the call site, because the
//!   destructive helpers (`remove_sidecar`, `remove_companion`, `reclaim_image`) deliberately
//!   take no project parameter — threading one through the [`ApfsExecutionHost`] trait would
//!   couple every substrate caller to journal layout. When the layout is unrecognized the
//!   entry is skipped, never guessed.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::metadata::WorkspaceName;

/// Per-project destruction evidence, stored beside `lifecycle-intents.json`.
pub const DELETION_LOG_FILE: &str = "deletion-log.jsonl";

/// What kind of artifact a log line records the removal of.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeletionKind {
    Companion,
    Sidecar,
    Image,
    Other,
}

/// The controller operation that performed the removal. Serialized kebab-case so doctor
/// messages can interpolate it verbatim ("removed by {op} at {at}").
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeletionOp {
    #[serde(rename = "remove-sidecar")]
    RemoveSidecar,
    #[serde(rename = "remove-companion")]
    RemoveCompanion,
    #[serde(rename = "reclaim-image")]
    ReclaimImage,
    #[serde(rename = "reclaim-retired-artifact")]
    ReclaimRetiredArtifact,
    #[serde(rename = "remove-staging-mount")]
    RemoveStagingMount,
    #[serde(rename = "remove-orphan-staging-metadata")]
    RemoveOrphanStagingMetadata,
    #[serde(rename = "remove-orphan-mountpoint")]
    RemoveOrphanMountpoint,
}

impl std::fmt::Display for DeletionOp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tag = match self {
            Self::RemoveSidecar => "remove-sidecar",
            Self::RemoveCompanion => "remove-companion",
            Self::ReclaimImage => "reclaim-image",
            Self::ReclaimRetiredArtifact => "reclaim-retired-artifact",
            Self::RemoveStagingMount => "remove-staging-mount",
            Self::RemoveOrphanStagingMetadata => "remove-orphan-staging-metadata",
            Self::RemoveOrphanMountpoint => "remove-orphan-mountpoint",
        };
        formatter.write_str(tag)
    }
}

/// One line of `deletion-log.jsonl`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeletionLogEntry {
    /// Zero-padded UTC timestamp; lexicographic order is chronological order.
    pub at: String,
    pub op: DeletionOp,
    /// Best-effort workspace name derived from the image path; empty when underivable.
    /// The classifier keys on paths, never on this field.
    pub workspace: String,
    /// The image the artifact belonged to. `None` for imageless removals (a stale
    /// mountpoint directory has no image by definition).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<PathBuf>,
    /// The exact path that was unlinked.
    pub artifact: PathBuf,
    pub kind: DeletionKind,
}

/// The controller's own record that it removed an image's CA companion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerRemoval {
    pub op: DeletionOp,
    pub at: String,
}

/// What the absence of a CA companion means. Data for doctor, not codes: doctor owns the
/// `ca-companion-missing` finding and interpolates these variants into its message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompanionAbsence {
    /// The deletion log holds a tombstone for this companion: the controller did it.
    RemovedByController(ControllerRemoval),
    /// A fenced publication is still in its crash window — the companion may never have
    /// been minted. The safe direction: resume/repair, never blame the outside world.
    CrashWindow,
    /// The fence completed (or no fence is on record) and no tombstone exists: whatever
    /// removed the companion was not the controller.
    RemovedExternally,
}

/// Best-effort append of one log line. Never fails, never panics: every I/O error is
/// swallowed so a broken log cannot fail the op it records.
///
/// The artifact family is a per-call statement, not derived from the op: a reclaim that
/// removes an image-family fact file states its kind explicitly instead of inheriting the
/// image kind, so readers never have to guess what was unlinked.
pub fn log_deletion(
    project_root: &Path,
    op: DeletionOp,
    kind: DeletionKind,
    workspace: &str,
    image: Option<&Path>,
    artifact: &Path,
) {
    let entry = DeletionLogEntry {
        at: rfc3339_utc(SystemTime::now()),
        op,
        workspace: workspace.to_owned(),
        image: image.map(Path::to_path_buf),
        artifact: artifact.to_path_buf(),
        kind,
    };
    append_entry(project_root, &entry);
}

/// Convenience for the static destructive helpers, which know only the image path.
/// Derives the project root and workspace lexically; skips silently when the image path
/// does not match a known store layout instead of logging to the wrong project.
pub fn log_deletion_for_image(image: &Path, op: DeletionOp, kind: DeletionKind, artifact: &Path) {
    let Some((project_root, workspace)) = project_and_workspace_for_image(image) else {
        return;
    };
    log_deletion(&project_root, op, kind, &workspace, Some(image), artifact);
}

/// Doctor-side lookup: why is this image's CA companion missing?
///
/// * A tombstone in the deletion log is ground truth — the controller removed it, and the
///   returned `{op, at}` names the operation for the finding message.
/// * Otherwise a still-open fence (`pending_fence` with incomplete or unknown steps) means
///   the controller may have died before minting the companion: crash window.
/// * Otherwise the companion existed (or no fence ever ran) and vanished without a
///   controller record: external deletion.
///
/// `fence` is `None` for pre-fencing journals, which deserialize the field as absent and
/// are treated exactly as before — an open fence with no step evidence is a crash window.
pub fn classify_missing_companion(
    project_root: &Path,
    image: &Path,
    fence: Option<crate::storage::recovery::FenceSteps>,
    pending_fence: bool,
) -> CompanionAbsence {
    if let Some(removal) = find_companion_removal(project_root, image) {
        return CompanionAbsence::RemovedByController(removal);
    }
    let fence_complete = fence.is_some_and(|steps| steps.is_complete());
    if pending_fence && !fence_complete {
        return CompanionAbsence::CrashWindow;
    }
    CompanionAbsence::RemovedExternally
}

/// Newest tombstone naming this image's companion, if any. Malformed lines are skipped —
/// one corrupt line must not blind the reader to the rest of the evidence.
fn find_companion_removal(project_root: &Path, image: &Path) -> Option<ControllerRemoval> {
    let bytes = std::fs::read(project_root.join(DELETION_LOG_FILE)).ok()?;
    let text = String::from_utf8_lossy(&bytes);
    let mut newest: Option<ControllerRemoval> = None;
    let mut newest_at = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<DeletionLogEntry>(line) else {
            continue;
        };
        if entry.kind != DeletionKind::Companion {
            continue;
        }
        let matches_image = entry.image.as_deref() == Some(image);
        let matches_artifact = entry.artifact == companion_path_for(image);
        if !matches_image && !matches_artifact {
            continue;
        }
        if newest.is_none() || entry.at > newest_at {
            newest_at = entry.at.clone();
            newest = Some(ControllerRemoval {
                op: entry.op,
                at: entry.at,
            });
        }
    }
    newest
}

/// Append with an fsync so a crash right after the unlink cannot lose the line that
/// explains it. Every error — open, write, sync — is swallowed by contract.
fn append_entry(project_root: &Path, entry: &DeletionLogEntry) {
    let Ok(mut line) = serde_json::to_vec(entry) else {
        return;
    };
    line.push(b'\n');
    let path = project_root.join(DELETION_LOG_FILE);
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) else {
        return;
    };
    if file.write_all(&line).is_err() {
        return;
    }
    let _ = file.sync_all();
    // Best-effort directory sync so the directory entry itself is durable. Failure here
    // loses nothing the file fsync did not already cover on most filesystems.
    if let Ok(directory) = std::fs::File::open(project_root) {
        let _ = directory.sync_all();
    }
}

/// Lexically locate the project root for an image path and derive its workspace name.
/// `None` when the image path does not match a known store layout; the workspace is always
/// best-effort (empty when underivable), the project root is load-bearing.
fn project_and_workspace_for_image(image: &Path) -> Option<(PathBuf, String)> {
    if !image.is_absolute() {
        return None;
    }
    let parent = image.parent()?;
    let container = parent.file_name()?.to_str()?;
    let stem = image.file_stem()?.to_str()?;
    match container {
        "sessions" => parent
            .parent()
            .map(|project| (project.to_path_buf(), stem.to_owned())),
        ".staging" | ".trash" => parent.parent().map(|project| {
            (
                project.to_path_buf(),
                staged_stem_workspace(stem).unwrap_or_default(),
            )
        }),
        _ => {
            // `<project>/checkpoints/<workspace>/<label>.<ext>`: the workspace is the
            // directory, the project two levels up.
            if parent
                .parent()
                .and_then(|grandparent| grandparent.file_name())
                .and_then(|name| name.to_str())
                == Some("checkpoints")
            {
                let workspace = parent
                    .file_name()
                    .and_then(|name| name.to_str())
                    .filter(|name| WorkspaceName::new(*name).is_ok())
                    .unwrap_or_default();
                return parent
                    .parent()
                    .and_then(|grandparent| grandparent.parent())
                    .map(|project| (project.to_path_buf(), workspace.to_owned()));
            }
            // `<project>/main.<ext>`: the parent is the project root itself.
            if WorkspaceName::new(stem).is_ok() {
                return Some((parent.to_path_buf(), stem.to_owned()));
            }
            None
        }
    }
}

/// The workspace half of a `<name>-<incarnation>` staging/trash stem. `None` when the
/// stem is not one staging ever produces — the caller records an empty workspace name
/// rather than guessing.
fn staged_stem_workspace(stem: &str) -> Option<String> {
    let (workspace, incarnation) = stem.rsplit_once('-')?;
    if incarnation.len() != 32
        || !incarnation.bytes().all(|byte| byte.is_ascii_hexdigit())
        || WorkspaceName::new(workspace).is_err()
    {
        return None;
    }
    Some(workspace.to_owned())
}

fn companion_path_for(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".ca.key");
    PathBuf::from(path)
}

fn rfc3339_utc(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    let (year, month, day) = crate::storage::civil_from_days(secs / 86_400);
    let clock = secs % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        clock / 3_600,
        (clock % 3_600) / 60,
        clock % 60,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_project(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "cowshed-deletion-log-{}-{}-{}",
            name,
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).expect("temp project");
        root
    }

    #[test]
    fn append_and_find_round_trip() {
        let project = temp_project("round-trip");
        let image = project.join("sessions/ws.sparseimage");
        let companion = companion_path_for(&image);
        log_deletion(
            &project,
            DeletionOp::RemoveCompanion,
            DeletionKind::Companion,
            "ws",
            Some(&image),
            &companion,
        );
        let found = find_companion_removal(&project, &image).expect("tombstone");
        assert_eq!(found.op, DeletionOp::RemoveCompanion);
        assert!(!found.at.is_empty());
        assert!(project.join(DELETION_LOG_FILE).is_file());
        let _ = std::fs::remove_dir_all(&project);
    }

    #[test]
    fn malformed_lines_do_not_hide_evidence() {
        let project = temp_project("malformed");
        let image = project.join("sessions/ws.sparseimage");
        let companion = companion_path_for(&image);
        std::fs::write(project.join(DELETION_LOG_FILE), b"{not json}\n\n").expect("corrupt prefix");
        log_deletion(
            &project,
            DeletionOp::ReclaimImage,
            DeletionKind::Companion,
            "ws",
            Some(&image),
            &companion,
        );
        // A reclaim-image line for the companion path still matches by artifact, even
        // though its op is not companion-specific: the artifact is what was unlinked.
        let found = find_companion_removal(&project, &image).expect("tombstone");
        assert_eq!(found.op, DeletionOp::ReclaimImage);
        let _ = std::fs::remove_dir_all(&project);
    }

    #[test]
    fn newest_tombstone_wins() {
        let project = temp_project("newest");
        let image = project.join("sessions/ws.sparseimage");
        let companion = companion_path_for(&image);
        let older = DeletionLogEntry {
            at: "2026-01-01T00:00:00Z".to_owned(),
            op: DeletionOp::RemoveCompanion,
            workspace: "ws".to_owned(),
            image: Some(image.clone()),
            artifact: companion.clone(),
            kind: DeletionKind::Companion,
        };
        let newer = DeletionLogEntry {
            at: "2026-06-01T00:00:00Z".to_owned(),
            op: DeletionOp::ReclaimImage,
            workspace: "ws".to_owned(),
            image: Some(image.clone()),
            artifact: companion.clone(),
            kind: DeletionKind::Companion,
        };
        append_entry(&project, &older);
        append_entry(&project, &newer);
        let found = find_companion_removal(&project, &image).expect("tombstone");
        assert_eq!(found.op, DeletionOp::ReclaimImage);
        assert_eq!(found.at, "2026-06-01T00:00:00Z");
        let _ = std::fs::remove_dir_all(&project);
    }

    #[test]
    fn missing_log_means_no_removal() {
        let project = temp_project("missing");
        let image = project.join("sessions/ws.sparseimage");
        assert!(find_companion_removal(&project, &image).is_none());
        let _ = std::fs::remove_dir_all(&project);
    }

    #[test]
    fn append_never_fails_on_unwritable_project() {
        let project = temp_project("read-only");
        let image = project.join("sessions/ws.sparseimage");
        // A directory where the log path cannot be created: the call must swallow the
        // error, never panic, so the op it records still succeeds.
        std::fs::create_dir_all(project.join(DELETION_LOG_FILE)).expect("block the log path");
        log_deletion(
            &project,
            DeletionOp::RemoveCompanion,
            DeletionKind::Companion,
            "ws",
            Some(&image),
            &companion_path_for(&image),
        );
        let _ = std::fs::remove_dir_all(&project);
    }

    fn project_layouts_resolve() {
        let root = PathBuf::from("/store/acme/widget");
        assert_eq!(
            project_and_workspace_for_image(&root.join("sessions/demo.sparseimage")),
            Some((root.clone(), "demo".to_owned())),
        );

        let incarnation = "00000000000000000000000000000001";
        assert_eq!(
            project_and_workspace_for_image(
                &root.join(format!(".staging/demo-{incarnation}.sparseimage")),
            ),
            Some((root.clone(), "demo".to_owned())),
        );
        assert_eq!(
            project_and_workspace_for_image(
                &root.join(format!("sessions/.trash/demo-{incarnation}.sparseimage")),
            ),
            Some((root.clone(), "demo".to_owned())),
        );
        assert_eq!(
            project_and_workspace_for_image(&root.join("checkpoints/demo/one.sparseimage")),
            Some((root.clone(), "demo".to_owned())),
        );
        assert_eq!(
            project_and_workspace_for_image(&root.join("main.sparseimage")),
            Some((root.clone(), "main".to_owned())),
        );

        assert_eq!(
            project_and_workspace_for_image(Path::new("relative/path.sparseimage")),
            None
        );
        assert_eq!(
            project_and_workspace_for_image(&root.join("noise.txt")),
            None
        );
    }
}
