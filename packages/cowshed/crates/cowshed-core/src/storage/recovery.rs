use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::api::dto::{AdoptOptions, CreateOptions, RemoveOptions, RemoveReport};
use crate::error::{CowshedError, Result as CowshedResult};
use crate::metadata::{MetadataError, WorkspaceIncarnation, WorkspaceName, read_json, write_json};
use crate::repository::RepoId;
/// Objects in these namespaces are controller implementation details and never canonical listings.
pub const STAGING_NAMESPACE: &str = ".staging";
pub const TRASH_NAMESPACE: &str = ".trash";
pub const LIFECYCLE_INTENTS_FILE: &str = "lifecycle-intents.json";
const LIFECYCLE_INTENT_VERSION: u32 = 1;

/// Durable user intent written before a lifecycle verb's first mutation.
///
/// Deliberately not [`super::lifecycle::Operation`], and not substitutable for it. An intent
/// records the caller's pre-plan API options so a crashed verb can be *replanned* against
/// whatever the store looks like on the next open; an `Operation` is the already-resolved,
/// capability-free plan that `revalidate` checks against reread facts immediately before
/// mutating. Feeding an intent where a plan is expected would execute without the expected-fact
/// set that makes execution refusable; feeding a plan where an intent is expected would persist
/// a decision made against facts that no longer hold. The overlap in their verb names is the
/// only thing the two share, and the on-disk `kind` tags here are schema, not vocabulary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LifecycleIntent {
    Adopt {
        options: AdoptOptions,
    },
    Create {
        workspace: WorkspaceName,
        options: CreateOptions,
    },
    Fork {
        source: WorkspaceName,
        destination: WorkspaceName,
    },
    Retire {
        workspace: WorkspaceName,
        options: RemoveOptions,
    },
}

impl LifecycleIntent {
    pub fn target(&self) -> &WorkspaceName {
        match self {
            Self::Adopt { .. } => main_name(),
            Self::Create { workspace, .. } | Self::Retire { workspace, .. } => workspace,
            Self::Fork { destination, .. } => destination,
        }
    }
}

/// The result needed to make an idempotent re-issue indistinguishable from the first call.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "result",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum LifecycleIntentCompletion {
    Workspace(WorkspaceIncarnation),
    Retire(RemoveReport),
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LifecycleIntentPhase {
    #[default]
    Prepared,
    Mutating,
}

impl LifecycleIntentPhase {
    fn is_prepared(&self) -> bool {
        *self == Self::Prepared
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LifecycleIntentRecord {
    pub operation: LifecycleIntent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<LifecycleIntentCompletion>,
    /// `Prepared` means no irreversible mutation has begun. Recovery may discard a prepared
    /// retirement; only `Mutating` retirement records carry enough evidence to resume deletion.
    #[serde(default, skip_serializing_if = "LifecycleIntentPhase::is_prepared")]
    pub phase: LifecycleIntentPhase,
}

impl LifecycleIntentRecord {
    pub fn pending(operation: LifecycleIntent) -> Self {
        Self {
            operation,
            completion: None,
            phase: LifecycleIntentPhase::Prepared,
        }
    }
}

/// One bounded record per logical workspace. A later lifecycle supersedes the prior record for the
/// same name, so retry evidence cannot grow without bound.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LifecycleIntentJournal {
    version: u32,
    entries: BTreeMap<WorkspaceName, LifecycleIntentRecord>,
}

impl Default for LifecycleIntentJournal {
    fn default() -> Self {
        Self {
            version: LIFECYCLE_INTENT_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

impl LifecycleIntentJournal {
    pub fn load(path: &Path) -> CowshedResult<Self> {
        let journal = match read_json(path) {
            Ok(journal) => journal,
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Self::default()
            }
            Err(error) => {
                return Err(CowshedError::integrity(
                    format!(
                        "cannot read lifecycle intent journal {}: {error}",
                        path.display()
                    ),
                    "repair the lifecycle intent journal, then reopen cowshed",
                ));
            }
        };
        journal.validate().map_err(|error| {
            let project = path.parent().unwrap_or(path);
            CowshedError::new(
                error.code,
                format!(
                    "invalid lifecycle intent journal for project {} in {}: {}",
                    project.display(),
                    path.display(),
                    error.message
                ),
                error.hint,
            )
        })?;
        Ok(journal)
    }

    pub fn persist(&self, path: &Path) -> CowshedResult<()> {
        self.validate()?;
        write_json(path, self).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot persist lifecycle intent journal {}: {error}",
                    path.display()
                ),
                "repair cowshed storage and retry the lifecycle operation",
            )
        })
    }

    pub fn get(&self, workspace: &WorkspaceName) -> Option<&LifecycleIntentRecord> {
        self.entries.get(workspace)
    }

    pub fn records(&self) -> impl Iterator<Item = (&WorkspaceName, &LifecycleIntentRecord)> {
        self.entries.iter()
    }

    pub fn begin(&mut self, operation: LifecycleIntent) {
        self.entries.insert(
            operation.target().clone(),
            LifecycleIntentRecord::pending(operation),
        );
    }
    pub fn mark_mutating(&mut self, workspace: &WorkspaceName) -> CowshedResult<()> {
        let record = self.entries.get_mut(workspace).ok_or_else(|| {
            CowshedError::integrity(
                format!("lifecycle intent for {workspace} disappeared before mutation"),
                "reopen cowshed to reconcile lifecycle state",
            )
        })?;
        if record.completion.is_some() {
            return Err(CowshedError::integrity(
                format!("completed lifecycle intent for {workspace} cannot begin mutation"),
                "reopen cowshed to reconcile lifecycle state",
            ));
        }
        record.phase = LifecycleIntentPhase::Mutating;
        self.validate()
    }

    /// Forgets one retirement which was proven not to have crossed its mutation fence.
    ///
    /// The caller must first observe that the target workspace still exists. Older journals have
    /// no phase field and deserialize as `Prepared`; host state, not journal age, decides whether
    /// those records are safe to discard.
    pub fn discard_prepared_retirement(&mut self, workspace: &WorkspaceName) -> bool {
        let discard = self.entries.get(workspace).is_some_and(|record| {
            record.completion.is_none()
                && record.phase == LifecycleIntentPhase::Prepared
                && matches!(record.operation, LifecycleIntent::Retire { .. })
        });
        if discard {
            self.entries.remove(workspace);
        }
        discard
    }

    pub fn complete(
        &mut self,
        workspace: &WorkspaceName,
        completion: LifecycleIntentCompletion,
    ) -> CowshedResult<()> {
        let record = self.entries.get_mut(workspace).ok_or_else(|| {
            CowshedError::integrity(
                format!("lifecycle intent for {workspace} disappeared before completion"),
                "reopen cowshed to reconcile lifecycle state",
            )
        })?;
        record.completion = Some(completion);
        self.validate()
    }

    pub fn clear(&mut self, workspace: &WorkspaceName) {
        self.entries.remove(workspace);
    }

    fn validate(&self) -> CowshedResult<()> {
        if self.version != LIFECYCLE_INTENT_VERSION {
            return Err(CowshedError::integrity(
                format!(
                    "unsupported lifecycle intent journal version {}",
                    self.version
                ),
                "upgrade cowshed or repair the lifecycle intent journal",
            ));
        }
        for (workspace, record) in &self.entries {
            if record.operation.target() != workspace {
                return Err(CowshedError::integrity(
                    format!(
                        "lifecycle intent record {workspace} disagrees with target {}",
                        record.operation.target()
                    ),
                    "repair the lifecycle intent journal, then reopen cowshed",
                ));
            }
            let valid_completion = matches!(
                (&record.operation, &record.completion),
                (_, None)
                    | (
                        LifecycleIntent::Adopt { .. }
                            | LifecycleIntent::Create { .. }
                            | LifecycleIntent::Fork { .. },
                        Some(LifecycleIntentCompletion::Workspace(_))
                    )
                    | (
                        LifecycleIntent::Retire { .. },
                        Some(LifecycleIntentCompletion::Retire(_))
                    )
            );
            if !valid_completion {
                return Err(CowshedError::integrity(
                    format!("lifecycle intent record {workspace} has an incompatible completion"),
                    "repair the lifecycle intent journal, then reopen cowshed",
                ));
            }
        }
        Ok(())
    }
}

/// Store-root intent for the one operation whose project directory changes name.
///
/// It lives at the store root rather than in `lifecycle-intents.json` because that journal moves
/// with the project directory this operation renames, and because the disagreement it describes —
/// a project directory and the identity in its binding naming different things — is store-wide
/// evidence that any reader of the store may need.
///
/// There is at most one at a time: an identity change holds the project actor exclusively, and a
/// second one cannot start while a first is unfinished because the first thing every open does is
/// finish it.
pub const REPO_IDENTITY_INTENT_FILE: &str = ".cowshed-repo-id-intent.json";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryIdentityIntent {
    pub old_repo_id: RepoId,
    pub new_repo_id: RepoId,
    /// The project's checkout, needed only to repoint its symlink under `CheckoutLayout::Symlink`.
    /// Recovery never gates on it: an unfinished identity change is store-wide damage, so it is
    /// completed by whichever open notices it first, not only by the renamed project's own.
    pub checkout_path: PathBuf,
    pub old_project_root: PathBuf,
    pub new_project_root: PathBuf,
    pub old_mount_root: PathBuf,
    pub new_mount_root: PathBuf,
    /// `Prepared` means nothing durable has been touched and the record may be discarded.
    /// `Mutating` is the mutation fence: it is written only once every volume is unmounted and the
    /// next act is a directory rename, so recovery from it is always forward.
    #[serde(default)]
    pub phase: LifecycleIntentPhase,
}

impl RepositoryIdentityIntent {
    /// Refuse anything that is not a well-formed identity change of this store.
    ///
    /// A malformed or truncated record is refused rather than skipped: it is evidence that a
    /// transaction was interrupted, and silently ignoring it would leave the store half-renamed
    /// with nothing left to say so.
    ///
    /// Both project roots must be inside the store, and the two mount roots must be siblings under
    /// one mount root — the workspace mount root is configurable and deliberately lives outside the
    /// store, so containment there is expressed as "the same place, a different name" rather than a
    /// prefix. Either way a record naming somewhere else can never make recovery rename a directory
    /// cowshed does not own.
    pub fn validate(&self, store_root: &Path) -> CowshedResult<()> {
        let paths = [
            ("checkout", &self.checkout_path, false),
            ("old project", &self.old_project_root, true),
            ("new project", &self.new_project_root, true),
            ("old mount", &self.old_mount_root, false),
            ("new mount", &self.new_mount_root, false),
        ];
        for (name, path, inside_store) in paths {
            if !path.is_absolute() {
                return Err(malformed_identity_intent(format!(
                    "repository identity intent has a relative {name} path {}",
                    path.display()
                )));
            }
            if inside_store && !path.starts_with(store_root) {
                return Err(malformed_identity_intent(format!(
                    "repository identity intent {name} path {} escapes store {}",
                    path.display(),
                    store_root.display()
                )));
            }
        }
        // `<mount-root>/<owner>/<repo>` on both sides, so the shared grandparent is the configured
        // mount root itself.
        if self.old_mount_root.parent().and_then(Path::parent)
            != self.new_mount_root.parent().and_then(Path::parent)
        {
            return Err(malformed_identity_intent(format!(
                "repository identity intent mount paths {} and {} are not under one mount root",
                self.old_mount_root.display(),
                self.new_mount_root.display()
            )));
        }
        if self.old_repo_id == self.new_repo_id
            || self.old_project_root == self.new_project_root
            || self.old_mount_root == self.new_mount_root
        {
            return Err(malformed_identity_intent(
                "repository identity intent does not describe a change".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn path(store_root: &Path) -> PathBuf {
        store_root.join(REPO_IDENTITY_INTENT_FILE)
    }

    pub fn persist(&self, store_root: &Path) -> CowshedResult<()> {
        self.validate(store_root)?;
        let path = Self::path(store_root);
        write_json(&path, self).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot persist repository identity intent {}: {error}",
                    path.display()
                ),
                "repair cowshed storage and retry `cowshed mv main --repo-id`",
            )
        })
    }

    pub fn load(store_root: &Path) -> CowshedResult<Option<Self>> {
        let path = Self::path(store_root);
        match read_json::<Self>(&path) {
            Ok(intent) => {
                intent.validate(store_root)?;
                Ok(Some(intent))
            }
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(error) => Err(malformed_identity_intent(format!(
                "cannot read repository identity intent {}: {error}",
                path.display()
            ))),
        }
    }

    pub fn clear(store_root: &Path) -> CowshedResult<()> {
        let path = Self::path(store_root);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CowshedError::environment_missing(
                format!(
                    "cannot clear repository identity intent {}: {error}",
                    path.display()
                ),
                "repair cowshed storage and retry `cowshed mv main --repo-id`",
            )),
        }
    }
}

fn malformed_identity_intent(message: String) -> CowshedError {
    CowshedError::integrity(
        message,
        "inspect the identity intent with `cowshed doctor` before removing it — a project may be \
         half-renamed",
    )
}

/// The refusal a retained `Prepared` retirement produces when its target state cannot be read.
///
/// macOS-only because its sole caller is the APFS lifecycle recovery path, which is itself
/// `cfg(target_os = "macos")`. Without the gate a Linux `cargo check` of the library alone sees
/// no caller and `-D dead-code` fails the build, while a local `--all-targets` run hides it
/// behind the test below.
#[cfg(target_os = "macos")]
pub(crate) fn prepared_retirement_unreadable(
    workspace: &WorkspaceName,
    expected_mount: &Path,
    cause: CowshedError,
) -> CowshedError {
    CowshedError::new(
        cause.code,
        format!(
            "cannot establish retirement state for workspace {workspace} at {}: {}",
            expected_mount.display(),
            cause.message
        ),
        format!(
            "retirement record for {workspace} was kept deliberately; make {} and its workspace \
             image readable, then reopen cowshed",
            expected_mount.display()
        ),
    )
}

#[cfg(all(test, target_os = "macos"))]
mod prepared_retirement_diagnostic_tests {
    use std::path::Path;

    use crate::error::{CowshedError, ErrorCode};
    use crate::metadata::WorkspaceName;

    use super::prepared_retirement_unreadable;

    #[test]
    fn unreadable_state_names_the_target_and_preserves_the_recovery_record() {
        let workspace = WorkspaceName::new("unreadable").expect("workspace");
        let error = prepared_retirement_unreadable(
            &workspace,
            Path::new("/tmp/cowshed/example/unreadable"),
            CowshedError::environment_missing(
                "workspace metadata could not be read",
                "repair the metadata",
            ),
        );

        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert!(error.message.contains("workspace unreadable"));
        assert!(error.message.contains("/tmp/cowshed/example/unreadable"));
        assert!(error.message.contains("metadata could not be read"));
        assert!(
            error
                .hint
                .contains("record for unreadable was kept deliberately")
        );
        assert!(error.hint.contains("make /tmp/cowshed/example/unreadable"));
    }
}

static MAIN_NAME: LazyLock<WorkspaceName> = LazyLock::new(WorkspaceName::main);

fn main_name() -> &'static WorkspaceName {
    &MAIN_NAME
}
