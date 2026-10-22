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
//! * A build volume's release that deletes its image writes one line, for the image, whatever else
//!   it deletes: the volume's sidecar record, hold file and mountpoint are its own bookkeeping,
//!   gone with it under the release's claim. The line says whether the sidecar recorded the volume
//!   as a seed and names the workspace it recorded, read before anything was deleted, because that
//!   is what nothing else can tell once the volume is gone. A release that finds the image already
//!   gone retires the sidecar record alone and says so in a line of its own,
//!   `release-imageless-build-volume`, whose artifact is that record and whose image is the missing
//!   path: a record retired because its image vanished is not a volume reclaimed.
//! * Git facts are written where they are known, never reconstructed. A workspace retirement
//!   (`retire-workspace`) is the one line that is not an unlink: the rename into trash is the
//!   moment the workspace's repository stops being reachable, and the line records its head and
//!   every local branch with what keeps each tip — a ref in main's repository, or the target that
//!   held it. Keeping an abandon bundle's tips before `gc --abandoned` deletes it
//!   (`keep-abandoned-tips`) records the refs the same way, under the bundle's path. Retiring an
//!   abandonment's refs from main's repository (`retire-abandoned-ref`) records each tip with what
//!   let it go: the target branch that holds it, or the reason its owner gave for superseding it. A
//!   workspace retirement also says who retired it (`retiredBy`): the process that ran the removal,
//!   its argv, and its parent's, the shell or agent that asked; and, when the removal was forced,
//!   which of the guards it overrode, each named — so a removal nobody expected can be traced to
//!   whoever asked for it.

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
    /// A build volume's image whose sidecar did not record it as a seed: a checkout's live
    /// volume, one recorded unlinked, or one with no readable sidecar.
    #[serde(rename = "build-volume")]
    BuildVolume,
    /// A build volume's image whose sidecar recorded it as a target's seed.
    #[serde(rename = "build-seed")]
    BuildSeed,
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
    #[serde(rename = "remove-abandoned-bundle")]
    RemoveAbandonedBundle,
    /// A build volume's sidecar record retired because its image was already gone: the line's
    /// artifact is the record, its image the path that no longer exists.
    #[serde(rename = "release-imageless-build-volume")]
    ReleaseImagelessBuildVolume,
    #[serde(rename = "release-build-volume")]
    ReleaseBuildVolume,
    #[serde(rename = "retire-workspace")]
    RetireWorkspace,
    #[serde(rename = "keep-abandoned-tips")]
    KeepAbandonedTips,
    #[serde(rename = "retire-abandoned-ref")]
    RetireAbandonedRef,
    /// A recovery image's file moved out of the store by `gc --recover`, to `recoveredTo`.
    #[serde(rename = "recover-retired-image")]
    RecoverRetiredImage,
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
            Self::RemoveAbandonedBundle => "remove-abandoned-bundle",
            Self::ReleaseImagelessBuildVolume => "release-imageless-build-volume",
            Self::ReleaseBuildVolume => "release-build-volume",
            Self::RetireWorkspace => "retire-workspace",
            Self::KeepAbandonedTips => "keep-abandoned-tips",
            Self::RetireAbandonedRef => "retire-abandoned-ref",
            Self::RecoverRetiredImage => "recover-retired-image",
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
    /// The exact path that was unlinked; for `retire-workspace`, the checkout that retired.
    pub artifact: PathBuf,
    pub kind: DeletionKind,
    /// The workspace's Git tips and what keeps each, for the lines that retire Git history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<GitTips>,
    /// Who retired a workspace, on its `retire-workspace` line; absent on every other line, and
    /// on retirement lines a build before it wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_by: Option<RetiredBy>,
    /// On a `retire-workspace` line whose retirement kept its image as recovery (`image` names
    /// the trash path), every reason it did; absent on every other line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained: Option<Vec<crate::api::dto::RecoveryCause>>,
    /// Where a `recover-retired-image` line's artifact went, outside the store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovered_to: Option<PathBuf>,
}

/// A retired repository's head and every tip it held, each with what keeps it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitTips {
    pub head: crate::api::dto::GitOid,
    /// `HEAD` first, then every local branch in name order.
    pub tips: Vec<KeptTip>,
}

/// One tip: `HEAD`, or a branch's short name.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeptTip {
    pub name: String,
    pub tip: crate::api::dto::GitOid,
    pub kept: KeptBy,
}

/// What keeps a tip once its workspace is gone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "camelCase")]
pub enum KeptBy {
    /// `reference` in the repository at `repository` holds the tip itself: main's, or for a
    /// purge the retained checkout the restore puts back, where the ref outlives the project.
    Ref {
        repository: PathBuf,
        reference: String,
    },
    /// The removal gate proved `branch` (at `head`), or a ref `cowshed push` wrote, holds the
    /// tip's changes (02_workspaces.md, `rm` step 1).
    Target {
        branch: String,
        head: crate::api::dto::GitOid,
    },
    /// Nothing keeps the tip: its owner retired it as superseded, giving `reason`
    /// (`gc --supersede`, 01_storage.md).
    Superseded { reason: String },
}

/// Who retired a workspace, and what its `--force` overrode (02_workspaces.md, `rm` step 1).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetiredBy {
    pub caller: Caller,
    /// Present exactly when the removal was forced, naming every guard the force overrode;
    /// empty when nothing needed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forced: Option<Vec<Overridden>>,
}

/// The process that ran a removal and the one that started it: the shell, script or agent
/// that asked. Evidence, read best-effort: a parent that cannot be read is left out.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Caller {
    pub pid: u32,
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<CallerProcess>,
}

/// One process by its pid and argv, each argument rendered lossily as UTF-8.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallerProcess {
    pub pid: u32,
    pub argv: Vec<String>,
}

impl Caller {
    /// This process, which runs the removal, and its parent.
    pub fn this_process() -> Self {
        // SAFETY: getppid has no preconditions and cannot fail.
        let parent = unsafe { libc::getppid() };
        Self {
            pid: std::process::id(),
            argv: std::env::args_os()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect(),
            parent: parent_process(parent),
        }
    }
}

#[cfg(target_os = "macos")]
fn parent_process(pid: libc::pid_t) -> Option<CallerProcess> {
    let arguments = cowshed_os::process::process_arguments(pid).ok()?;
    Some(CallerProcess {
        pid: u32::try_from(pid).ok()?,
        argv: arguments
            .argv
            .iter()
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect(),
    })
}

#[cfg(not(target_os = "macos"))]
fn parent_process(pid: libc::pid_t) -> Option<CallerProcess> {
    let raw = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    Some(CallerProcess {
        pid: u32::try_from(pid).ok()?,
        argv: raw
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect(),
    })
}

/// A guard a forced removal overrode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "guard", rename_all = "camelCase")]
pub enum Overridden {
    /// The tree held uncommitted work.
    Dirty,
    /// A Git operation was in progress.
    InProgress { operation: String },
    /// The workspace was younger than the removal age floor.
    #[serde(rename_all = "camelCase")]
    Young {
        created_at: String,
        /// `None` when the workspace's creation time could not be read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        age_seconds: Option<u64>,
    },
}

/// A retirement that kept its image as recovery: the trash path it kept, and every reason.
pub struct KeptRecovery {
    pub image: PathBuf,
    pub because: Vec<crate::api::dto::RecoveryCause>,
}

/// Append a workspace retirement's line (`retire-workspace`): its Git tips when they were read,
/// who retired it, and, when it kept its image as recovery, where and why. Best-effort, under the
/// same never-fail rule as [`log_deletion`].
pub fn log_retirement(
    project_root: &Path,
    workspace: &str,
    artifact: &Path,
    git: Option<GitTips>,
    retired_by: RetiredBy,
    recovery: Option<KeptRecovery>,
) {
    let (image, retained) = match recovery {
        Some(KeptRecovery { image, because }) => (Some(image), Some(because)),
        None => (None, None),
    };
    let entry = DeletionLogEntry {
        at: rfc3339_utc(SystemTime::now()),
        op: DeletionOp::RetireWorkspace,
        workspace: workspace.to_owned(),
        image,
        artifact: artifact.to_path_buf(),
        kind: DeletionKind::Other,
        git,
        retired_by: Some(retired_by),
        retained,
        recovered_to: None,
    };
    append_entry(project_root, &entry);
}

/// Append one `recover-retired-image` line: `gc --recover` moved `artifact`, a file of the
/// recovery image `image`, out of the store to `recovered_to`. Best-effort, under the same
/// never-fail rule as [`log_deletion`].
pub fn log_recovery(
    project_root: &Path,
    workspace: &str,
    image: &Path,
    artifact: &Path,
    kind: DeletionKind,
    recovered_to: &Path,
) {
    let entry = DeletionLogEntry {
        at: rfc3339_utc(SystemTime::now()),
        op: DeletionOp::RecoverRetiredImage,
        workspace: workspace.to_owned(),
        image: Some(image.to_path_buf()),
        artifact: artifact.to_path_buf(),
        kind,
        git: None,
        retired_by: None,
        retained: None,
        recovered_to: Some(recovered_to.to_path_buf()),
    };
    append_entry(project_root, &entry);
}

/// Why the retirement that left the recovery image `image` kept it: the newest
/// `retire-workspace` line naming that trash path with its reasons. Malformed lines are skipped,
/// and a log that cannot be read records nothing.
pub fn recovery_retention(project_root: &Path, image: &Path) -> crate::api::dto::RecoveryRetention {
    let Ok(bytes) = std::fs::read(project_root.join(DELETION_LOG_FILE)) else {
        return crate::api::dto::RecoveryRetention::Unrecorded;
    };
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| serde_json::from_str::<DeletionLogEntry>(line.trim()).ok())
        .filter(|entry| {
            entry.op == DeletionOp::RetireWorkspace && entry.image.as_deref() == Some(image)
        })
        .filter_map(|entry| entry.retained.map(|because| (entry.at, because)))
        .max_by(|(left, _), (right, _)| left.cmp(right))
        .map_or(
            crate::api::dto::RecoveryRetention::Unrecorded,
            |(at, because)| crate::api::dto::RecoveryRetention::Recorded { at, because },
        )
}

/// Best-effort append of a line carrying Git facts, under the same never-fail rule as
/// [`log_deletion`].
pub fn log_git_tips(
    project_root: &Path,
    op: DeletionOp,
    workspace: &str,
    artifact: &Path,
    git: GitTips,
) {
    let entry = DeletionLogEntry {
        at: rfc3339_utc(SystemTime::now()),
        op,
        workspace: workspace.to_owned(),
        image: None,
        artifact: artifact.to_path_buf(),
        kind: DeletionKind::Other,
        git: Some(git),
        retired_by: None,
        retained: None,
        recovered_to: None,
    };
    append_entry(project_root, &entry);
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
        git: None,
        retired_by: None,
        retained: None,
        recovered_to: None,
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
        ".staging" => parent.parent().map(|project| {
            (
                project.to_path_buf(),
                staged_stem_workspace(stem).unwrap_or_default(),
            )
        }),
        // Trash lives one level deeper: `<project>/sessions/.trash/<file>`.
        ".trash" => parent
            .parent()
            .and_then(|sessions| sessions.parent())
            .map(|project| {
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
            // `<project>/main.<ext>`: the parent is the project root itself. Only the
            // literal main stem qualifies — any other filename here is not a layout
            // this module knows, and guessing would misplace the evidence.
            if stem == "main" {
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

pub(crate) fn rfc3339_utc(now: SystemTime) -> String {
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
        let image = project.join("sessions/ws.dmg");
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
        let image = project.join("sessions/ws.dmg");
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

    /// A recovery image's reason is the newest `retire-workspace` line naming its trash path with
    /// one; a line for another image, or a release that kept nothing, says nothing about it.
    #[test]
    fn a_recovery_image_reads_why_its_newest_retirement_kept_it() {
        use crate::api::dto::{RecoveryCause, RecoveryRetention};
        let project = temp_project("retention");
        let image = project.join("sessions/.trash/ws-0123456789abcdef0123456789abcdef.dmg");
        assert_eq!(
            recovery_retention(&project, &image),
            RecoveryRetention::Unrecorded
        );
        let retired_by = || RetiredBy {
            caller: Caller {
                pid: 1,
                argv: vec!["cowshed".to_owned(), "rm".to_owned()],
                parent: None,
            },
            forced: None,
        };
        log_retirement(
            &project,
            "ws",
            &project.join("mnt/ws"),
            None,
            retired_by(),
            None,
        );
        log_retirement(
            &project,
            "other",
            &project.join("mnt/other"),
            None,
            retired_by(),
            Some(KeptRecovery {
                image: project.join("sessions/.trash/other-0123456789abcdef0123456789abcdef.dmg"),
                because: vec![RecoveryCause::Dirty],
            }),
        );
        assert_eq!(
            recovery_retention(&project, &image),
            RecoveryRetention::Unrecorded
        );
        log_retirement(
            &project,
            "ws",
            &project.join("mnt/ws"),
            None,
            retired_by(),
            Some(KeptRecovery {
                image: image.clone(),
                because: vec![
                    RecoveryCause::Abandoned,
                    RecoveryCause::OnlyCopy {
                        path: PathBuf::from("notes/only.txt"),
                    },
                ],
            }),
        );
        let RecoveryRetention::Recorded { because, .. } = recovery_retention(&project, &image)
        else {
            panic!("the retirement's line names the image");
        };
        assert_eq!(
            because,
            [
                RecoveryCause::Abandoned,
                RecoveryCause::OnlyCopy {
                    path: PathBuf::from("notes/only.txt")
                }
            ]
        );
        let _ = std::fs::remove_dir_all(&project);
    }

    #[test]
    fn newest_tombstone_wins() {
        let project = temp_project("newest");
        let image = project.join("sessions/ws.dmg");
        let companion = companion_path_for(&image);
        let older = DeletionLogEntry {
            at: "2026-01-01T00:00:00Z".to_owned(),
            op: DeletionOp::RemoveCompanion,
            workspace: "ws".to_owned(),
            image: Some(image.clone()),
            artifact: companion.clone(),
            kind: DeletionKind::Companion,
            git: None,
            retired_by: None,
            retained: None,
            recovered_to: None,
        };
        let newer = DeletionLogEntry {
            at: "2026-06-01T00:00:00Z".to_owned(),
            op: DeletionOp::ReclaimImage,
            workspace: "ws".to_owned(),
            image: Some(image.clone()),
            artifact: companion.clone(),
            kind: DeletionKind::Companion,
            git: None,
            retired_by: None,
            retained: None,
            recovered_to: None,
        };
        append_entry(&project, &older);
        append_entry(&project, &newer);
        let found = find_companion_removal(&project, &image).expect("tombstone");
        assert_eq!(found.op, DeletionOp::ReclaimImage);
        assert_eq!(found.at, "2026-06-01T00:00:00Z");
        let _ = std::fs::remove_dir_all(&project);
    }
    /// A retirement line names who retired it and what a force overrode; a line a build before
    /// it wrote has neither and still reads.
    #[test]
    fn a_retirement_line_names_its_caller_and_what_force_overrode() {
        let entry: DeletionLogEntry = serde_json::from_str(
            r#"{"at":"2026-01-01T00:00:00Z","op":"retire-workspace","workspace":"ws","artifact":"/m/ws","kind":"other"}"#,
        )
        .expect("a line without retiredBy reads");
        assert_eq!(entry.retired_by, None);
        let by = RetiredBy {
            caller: Caller::this_process(),
            forced: Some(vec![
                Overridden::Dirty,
                Overridden::Young {
                    created_at: "2026-01-01T00:00:00Z".to_owned(),
                    age_seconds: Some(42),
                },
            ]),
        };
        assert_eq!(by.caller.pid, std::process::id());
        assert_eq!(
            serde_json::to_value(&by.forced).unwrap(),
            serde_json::json!([
                { "guard": "dirty" },
                { "guard": "young", "createdAt": "2026-01-01T00:00:00Z", "ageSeconds": 42 },
            ])
        );
    }

    #[test]
    fn missing_log_means_no_removal() {
        let project = temp_project("missing");
        let image = project.join("sessions/ws.dmg");
        assert!(find_companion_removal(&project, &image).is_none());
        let _ = std::fs::remove_dir_all(&project);
    }

    #[test]
    fn append_never_fails_on_unwritable_project() {
        let project = temp_project("read-only");
        let image = project.join("sessions/ws.dmg");
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

    #[test]
    fn project_layouts_resolve() {
        let root = PathBuf::from("/store/acme/widget");
        assert_eq!(
            project_and_workspace_for_image(&root.join("sessions/demo.dmg")),
            Some((root.clone(), "demo".to_owned())),
        );

        let incarnation = "00000000000000000000000000000001";
        assert_eq!(
            project_and_workspace_for_image(&root.join(format!(".staging/demo-{incarnation}.dmg")),),
            Some((root.clone(), "demo".to_owned())),
        );
        assert_eq!(
            project_and_workspace_for_image(
                &root.join(format!("sessions/.trash/demo-{incarnation}.dmg")),
            ),
            Some((root.clone(), "demo".to_owned())),
        );
        assert_eq!(
            project_and_workspace_for_image(&root.join("checkpoints/demo/one.dmg")),
            Some((root.clone(), "demo".to_owned())),
        );
        assert_eq!(
            project_and_workspace_for_image(&root.join("main.dmg")),
            Some((root.clone(), "main".to_owned())),
        );

        assert_eq!(
            project_and_workspace_for_image(Path::new("relative/path.dmg")),
            None
        );
        assert_eq!(
            project_and_workspace_for_image(&root.join("noise.txt")),
            None
        );
    }
}
