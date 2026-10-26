//! Lifecycle vocabulary: the checkpoint label grammar and the revision, fact and
//! mount values lifecycle plans are made against. Planners and substrates consume
//! these values; they live with neither.

use std::fmt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::clock::civil_from_days;
use crate::metadata::{WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use crate::repository::RepoId;

/// The label text a checkpoint constructor refused.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid checkpoint label {0:?}")]
pub struct InvalidCheckpointLabel(pub String);

/// A workspace name and role that disagree about whether the workspace is main.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("workspace {name} does not match role {role:?}")]
pub struct RoleNameMismatch {
    pub name: WorkspaceName,
    pub role: WorkspaceRole,
}
/// The undo-image grammar [`CheckpointLabel`] refuses and the restorer writes. One constant,
/// because a label admitted here that later collides with a controller undo image would
/// silently overwrite the only crash-consistent copy of a workspace.
pub const PRE_RESTORE_PREFIX: &str = "pre-restore-";
/// A validated, path-safe checkpoint label.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointLabel(String);

impl CheckpointLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidCheckpointLabel> {
        let value = value.into();
        let bytes = value.as_bytes();
        let valid = (1..=128).contains(&bytes.len())
            && !value.starts_with(PRE_RESTORE_PREFIX)
            && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
            && bytes.iter().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            });
        if valid {
            Ok(Self(value))
        } else {
            Err(InvalidCheckpointLabel(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The default label for an unlabeled checkpoint: the UTC second of the request, rendered in
    /// the label alphabet (`2026-07-11t120309z`). Workspace revision is the wrong key — it does
    /// not advance between checkpoints, so a revision-derived default collides deterministically
    /// on the second unlabeled checkpoint. A checkpoint image may be the only crash-consistent
    /// copy, so an existing label is never reused or overwritten: a same-second collision takes
    /// `-2`, `-3`, … instead.
    pub fn utc_default(now: SystemTime, mut is_taken: impl FnMut(&str) -> bool) -> Self {
        let seconds = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let (year, month, day) = civil_from_days(seconds / 86_400);
        let clock = seconds % 86_400;
        let base = format!(
            "{year:04}-{month:02}-{day:02}t{:02}{:02}{:02}z",
            clock / 3_600,
            clock % 3_600 / 60,
            clock % 60,
        );
        if !is_taken(&base) {
            return Self(base);
        }
        (2_u64..)
            .map(|ordinal| format!("{base}-{ordinal}"))
            .find(|candidate| !is_taken(candidate))
            .map(Self)
            .expect("existing checkpoint labels are finite")
    }
}

impl fmt::Display for CheckpointLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for CheckpointLabel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CheckpointLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn labels_are_immutable_validated_components() {
        for valid in ["a", "ci-fail", "2026-07-11t120000z", "release_1.2"] {
            let label = CheckpointLabel::new(valid).unwrap();
            assert_eq!(label.to_string(), valid);
            let encoded = serde_json::to_string(&label).unwrap();
            assert_eq!(
                serde_json::from_str::<CheckpointLabel>(&encoded).unwrap(),
                label
            );
        }
        for invalid in [
            "",
            ".",
            "..",
            "Upper",
            "-leading",
            "slash/name",
            "a b",
            "pre-restore-user",
            "pre-restore-00000000000000000000000000000002",
        ] {
            assert!(
                CheckpointLabel::new(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    fn at(seconds: u64) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(seconds)
    }

    #[test]
    fn the_default_label_is_the_documented_utc_second() {
        for (seconds, expected) in [
            (0, "1970-01-01t000000z"),
            (1_783_771_200, "2026-07-11t120000z"),
            // Leap day and both neighbours: a leap-year error shifts one of the three.
            (1_835_395_199, "2028-02-28t235959z"),
            (1_835_395_200, "2028-02-29t000000z"),
            (1_835_481_600, "2028-03-01t000000z"),
            // Year boundary at exactly midnight UTC.
            (1_893_455_999, "2029-12-31t235959z"),
            (1_893_456_000, "2030-01-01t000000z"),
            // 2100 is the century rule's first bite: divisible by 4 but not a leap year.
            (4_107_542_399, "2100-02-28t235959z"),
            (4_107_542_400, "2100-03-01t000000z"),
            // 2400 restores the 400-year exception, so its February 29th exists.
            (13_574_585_228, "2400-02-29t060708z"),
        ] {
            let label = CheckpointLabel::utc_default(at(seconds), |_| false);
            assert_eq!(label.as_str(), expected, "at {seconds}s");
            // Every generated default must survive the validator it bypasses.
            assert_eq!(CheckpointLabel::new(expected).unwrap(), label);
        }
    }

    #[test]
    fn a_same_second_collision_takes_the_next_ordinal_and_never_reuses_a_label() {
        let now = at(1_783_771_200);
        let base = CheckpointLabel::utc_default(now, |_| false);
        assert_eq!(base.as_str(), "2026-07-11t120000z");

        let taken = [base.as_str().to_owned()];
        let second =
            CheckpointLabel::utc_default(now, |candidate| taken.iter().any(|l| l == candidate));
        assert_eq!(second.as_str(), "2026-07-11t120000z-2");

        let taken = [taken[0].clone(), second.as_str().to_owned()];
        let third =
            CheckpointLabel::utc_default(now, |candidate| taken.iter().any(|l| l == candidate));
        assert_eq!(third.as_str(), "2026-07-11t120000z-3");
        assert_eq!(CheckpointLabel::new(third.as_str()).unwrap(), third);
    }
}
/// A monotonic controller-visible revision.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Revision(u64);

impl Revision {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0
    }
}
/// Logical identity and the exact version a lifecycle plan was made against.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleWorkspace {
    repo: RepoId,
    name: WorkspaceName,
    incarnation: WorkspaceIncarnation,
    revision: Revision,
    topology_revision: Revision,
    role: WorkspaceRole,
}

impl LifecycleWorkspace {
    pub fn new(
        repo: RepoId,
        name: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        revision: Revision,
        topology_revision: Revision,
        role: WorkspaceRole,
    ) -> Result<Self, RoleNameMismatch> {
        if name.is_main() != (role == WorkspaceRole::Main) {
            return Err(RoleNameMismatch { name, role });
        }
        Ok(Self {
            repo,
            name,
            incarnation,
            revision,
            topology_revision,
            role,
        })
    }
    pub fn repo(&self) -> &RepoId {
        &self.repo
    }
    pub fn name(&self) -> &WorkspaceName {
        &self.name
    }
    pub fn incarnation(&self) -> &WorkspaceIncarnation {
        &self.incarnation
    }
    pub const fn revision(&self) -> Revision {
        self.revision
    }
    pub const fn topology_revision(&self) -> Revision {
        self.topology_revision
    }
    pub const fn role(&self) -> WorkspaceRole {
        self.role
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRef {
    workspace: LifecycleWorkspace,
    label: CheckpointLabel,
    revision: Revision,
    pinned: bool,
}

impl CheckpointRef {
    pub fn new(
        workspace: LifecycleWorkspace,
        label: CheckpointLabel,
        revision: Revision,
        pinned: bool,
    ) -> Self {
        Self {
            workspace,
            label,
            revision,
            pinned,
        }
    }
    pub fn workspace(&self) -> &LifecycleWorkspace {
        &self.workspace
    }
    pub fn label(&self) -> &CheckpointLabel {
        &self.label
    }
    pub const fn revision(&self) -> Revision {
        self.revision
    }
    pub const fn pinned(&self) -> bool {
        self.pinned
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pin {
    Pinned,
    Automatic,
}
/// One authoritative lifecycle fact, whether a planner asserted it or a substrate observed it.
///
/// Expectation and observation are the same fact at two moments, so they are one type: a plan
/// carries the facts it was made against, the substrate rereads the facts under exclusion, and
/// revalidation is `==`. Two structurally identical enums made a mixed expected/observed pair
/// representable, cost a 40-line field-by-field match to reject, and turned "add a field to one,
/// forget the other" into a silent stale-plan miss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleFact {
    Exists {
        repo: RepoId,
        name: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        revision: Revision,
        topology_revision: Revision,
        retired: bool,
    },
    Absent {
        repo: RepoId,
        name: WorkspaceName,
        topology_revision: Revision,
    },
    Checkpoint {
        repo: RepoId,
        workspace: WorkspaceName,
        label: CheckpointLabel,
        revision: Revision,
    },
}

impl LifecycleFact {
    pub fn active(workspace: &LifecycleWorkspace) -> Self {
        Self::Exists {
            repo: workspace.repo.clone(),
            name: workspace.name.clone(),
            incarnation: workspace.incarnation.clone(),
            revision: workspace.revision,
            topology_revision: workspace.topology_revision,
            retired: false,
        }
    }
}

/// A structured stale-plan refusal. Executors return this before mutation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum Conflict {
    #[error("authoritative state has {actual} facts, expected {expected}")]
    FactCount { expected: usize, actual: usize },
    #[error("authoritative fact {index} changed")]
    Stale {
        index: usize,
        expected: Box<LifecycleFact>,
        actual: Box<LifecycleFact>,
    },
}

pub fn revalidate(expected: &[LifecycleFact], actual: &[LifecycleFact]) -> Result<(), Conflict> {
    if expected.len() != actual.len() {
        return Err(Conflict::FactCount {
            expected: expected.len(),
            actual: actual.len(),
        });
    }
    for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
        if expected != actual {
            return Err(Conflict::Stale {
                index,
                expected: Box::new(expected.clone()),
                actual: Box::new(actual.clone()),
            });
        }
    }
    Ok(())
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointFact {
    pub repo: RepoId,
    pub workspace: WorkspaceName,
    pub label: CheckpointLabel,
    pub revision: Revision,
    pub pin: Pin,
}
/// Where a published workspace stands against the kernel mount table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountState {
    Detached,
    Mounted {
        mount_id: u64,
    },
    /// Something is mounted at the workspace's mountpoint that does not prove itself the
    /// workspace's (01a_apfs.md, "Ownership, identity, and the volume label"): enumeration refused
    /// to join it. It is neither attached nor detached. Nothing acts on its absence, and an
    /// operation that names it refuses with this refusal.
    Unproven(Box<MountRefusal>),
}
/// Canonical persistent fact read from an image/dataset and its sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageFact {
    pub workspace: LifecycleWorkspace,
    pub volume_key: String,
}

/// A kernel mount-table fact supplied by the platform adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelMountFact {
    pub mount_id: u64,
    pub volume_key: String,
}

/// What enumeration made of the mount at one published workspace's mountpoint. Joining is judged
/// per workspace (01a_apfs.md, "Ownership, identity, and the volume label": joining a kernel mount
/// into enumeration reads the marker and refuses when it does not name the expected workspace):
/// one workspace's refusal is that workspace's fact, never a failure of every other workspace's
/// enumeration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountJoin {
    Proven(KernelMountFact),
    Refused {
        volume_key: String,
        refusal: Box<MountRefusal>,
    },
}

impl MountJoin {
    pub fn volume_key(&self) -> &str {
        match self {
            Self::Proven(fact) => &fact.volume_key,
            Self::Refused { volume_key, .. } => volume_key,
        }
    }

    /// The join an operation that names this one workspace may act on: a refusal fails exactly
    /// the operation that names the refused workspace.
    pub fn named(self) -> Result<KernelMountFact, Box<MountRefusal>> {
        match self {
            Self::Proven(fact) => Ok(fact),
            Self::Refused { refusal, .. } => Err(refusal),
        }
    }
}

impl From<KernelMountFact> for MountJoin {
    fn from(fact: KernelMountFact) -> Self {
        Self::Proven(fact)
    }
}

/// A mount at `mount_point`, the mountpoint of `workspace`, that enumeration refused to join.
/// `mounted_from` is what the mount table or directory binding says is mounted there.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{}", refusal_message(self))]
pub struct MountRefusal {
    pub workspace: LifecycleWorkspace,
    pub mount_point: PathBuf,
    pub mounted_from: String,
    pub reason: MountRefusalReason,
}

/// Why a mount was not joined. The marker reasons are the identity authority's two answers: a
/// marker that cannot be read is not ours, and one that reads names someone else.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountRefusalReason {
    /// The marker could not be read; the error is the one the read returned.
    MarkerUnreadable(String),
    /// The marker reads, and names another workspace or another incarnation.
    MarkerNamesAnother {
        repo: RepoId,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    },
    /// The workspace's own volume is mounted ignoring owners.
    NonCanonicalFlags(u64),
    /// A directory payload whose binding names another directory than its mountpoint.
    MountedElsewhere(PathBuf),
}

fn refusal_message(refusal: &MountRefusal) -> String {
    let MountRefusal {
        workspace,
        mount_point,
        mounted_from: source,
        reason,
    } = refusal;
    let (repo, name, incarnation) = (workspace.repo(), workspace.name(), workspace.incarnation());
    let at = mount_point.display();
    match reason {
        MountRefusalReason::MarkerUnreadable(error) => format!(
            "workspace mount identity mismatch at {at}: the volume mounted from {source} carries no readable {repo}/{name} marker for incarnation {incarnation}: {error}"
        ),
        MountRefusalReason::MarkerNamesAnother {
            repo: found_repo,
            workspace: found,
            incarnation: found_incarnation,
        } => format!(
            "workspace mount identity mismatch at {at}: the volume mounted from {source} carries the {found_repo}/{found} marker for incarnation {found_incarnation}, not {repo}/{name} for incarnation {incarnation}"
        ),
        MountRefusalReason::NonCanonicalFlags(flags) => format!(
            "workspace mount has non-canonical flags at {at}: it ignores owners, which every workspace mount must honour; flags={flags:#x}"
        ),
        MountRefusalReason::MountedElsewhere(current) => format!(
            "workspace mount identity mismatch: {source} is mounted at {}, its mountpoint is {at}, and the tree there must carry the {repo}/{name} marker for incarnation {incarnation}",
            current.display()
        ),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedWorkspace {
    pub workspace: LifecycleWorkspace,
    pub mount_state: MountState,
    pub checkpoints: Vec<CheckpointFact>,
}
