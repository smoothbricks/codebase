use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable cowshed outcome taxonomy shared by the core API and CLI.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorCode {
    Internal,
    Usage,
    NotFound,
    Conflict,
    EnvironmentMissing,
    SandboxDenied,
    Integrity,
}

impl ErrorCode {
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Internal => 1,
            Self::Usage => 2,
            Self::NotFound => 3,
            Self::Conflict => 4,
            Self::EnvironmentMissing => 5,
            Self::SandboxDenied => 6,
            Self::Integrity => 7,
        }
    }

    pub const fn exec_wrapper_exit_code(self) -> u8 {
        match self {
            Self::Internal => 100,
            Self::Usage => 101,
            Self::NotFound => 102,
            Self::Conflict => 103,
            Self::EnvironmentMissing => 104,
            Self::SandboxDenied => 105,
            Self::Integrity => 106,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::Usage => "usage",
            Self::NotFound => "not-found",
            Self::Conflict => "conflict",
            Self::EnvironmentMissing => "environment-missing",
            Self::SandboxDenied => "sandbox-denied",
            Self::Integrity => "integrity",
        }
    }
}

/// An operational error with a concrete recovery command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CowshedError {
    pub code: ErrorCode,
    pub message: String,
    pub hint: String,
    /// Present only on the daemon's refusal of another build's request. Absent from the wire
    /// otherwise, and ignored by a build that predates it, so every other error reads as it did.
    /// Boxed: every `Result` in cowshed carries this type, and the cause is rare.
    #[serde(
        rename = "otherBuild",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    other_build: Option<Box<OtherBuild>>,
    /// Present only on the daemon's refusal of a workspace whose supervisor from before it
    /// started it is still recovering; absent from the wire otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovering: Option<cowshed_gateway_types::SupervisorRecovery>,
    /// Present only on the daemon's refusal of a request that depends on the mounts and sessions
    /// its startup pass has not finished restoring; absent from the wire otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    healing: Option<cowshed_gateway_types::StartupHeal>,
    /// Present only on a rebase or land refused at one of its fences, carrying the value the
    /// fence observed, so a caller types the outcome without reading the repositories again.
    /// Absent from the wire otherwise, like `otherBuild`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "known_to_this_build"
    )]
    fence: Option<Box<FenceRefusal>>,
    /// Present only on a refusal that is safe to retry unchanged, naming why. Absent from the
    /// wire otherwise, like `otherBuild`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "known_to_this_build"
    )]
    retry: Option<Retry>,
    /// Boxed like `otherBuild` and `fence`: the structured CAS refusal is rare, and every
    /// `Result` in cowshed carries this type.
    #[serde(skip)]
    lifecycle_conflict: Option<Box<crate::storage::lifecycle::Conflict>>,
}

/// The daemon serves only its own build, and refused a request from another (11_shell.md
/// "hello"). A controller of the refused build can start nothing in a workspace from then on; a
/// controller of the daemon's build — the host's current `cowshed` once an install has started
/// its daemon — can. Unknown fields are ignored, so a later build may say more here without an
/// earlier one losing the error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OtherBuild {
    /// The build the daemon runs.
    pub daemon: crate::runtime::supervisor_socket::BuildId,
    /// The build the request named; `None` for a request that named none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<crate::runtime::supervisor_socket::BuildId>,
}

/// The structured source, or none when it names a reason this build does not know: version skew
/// between a controller and its client costs the typed reason, never the code, message and hint.
fn known_to_this_build<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    Ok(Option::<serde_json::Value>::deserialize(deserializer)?
        .and_then(|source| serde_json::from_value(source).ok()))
}

/// Why a `Conflict` is safe to retry unchanged: the call planned against state a concurrent
/// operation changed before it acted, and changed nothing itself. Retrying is the remedy; no
/// other repair is needed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "reason", rename_all = "camelCase")]
pub enum Retry {
    /// Garbage collection found the store changed between its plan and its execution (another
    /// process reclaiming or retiring an image) and collected nothing.
    GcPlanStale,
}

/// The most paths a [`FenceRefusal`] names; `total` still counts every one.
pub const MAX_FENCE_PATHS: usize = 64;

/// Why a rebase or land refused at one of its fences (02_workspaces.md), with what the fence
/// observed. Every variant is a `Conflict` that left the source workspace and the target as they
/// were; the observed value is what a caller would otherwise read back to decide its next move.
/// `IncarnationMoved` also types every other exact-incarnation refusal, and `SourceMoved` push's
/// source-head refusal: the same fence, wherever it stands.
///
/// Fields are additive like [`OtherBuild`]'s; a reason a later build added decodes as no fence
/// (`CowshedError::fence_source` answers `None`) rather than losing the whole error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum FenceRefusal {
    /// `workspace` is no longer the incarnation the caller expected (the source's
    /// `expected_workspace_incarnation`) or resolved (a lane base passed as `into`).
    IncarnationMoved {
        workspace: crate::metadata::WorkspaceName,
        observed: crate::metadata::WorkspaceIncarnation,
    },
    /// The source workspace's head is not the one expected or validated.
    SourceMoved { observed: crate::api::dto::GitOid },
    /// The rebase destination resolved to another commit than `expected_onto_head`.
    OntoMoved { observed: crate::api::dto::GitOid },
    /// The land target branch is not at `expected_target_head`; `None` when it does not exist.
    TargetMoved {
        observed: Option<crate::api::dto::GitOid>,
    },
    /// The target branch is at `target_head`, which the source is not based on: rebase first.
    NotFastForward {
        target_head: crate::api::dto::GitOid,
    },
    /// The target checkout has another branch checked out, or none (`None`: a detached HEAD).
    TargetNotCheckedOut { checked_out: Option<String> },
    /// The source workspace holds uncommitted work: up to [`MAX_FENCE_PATHS`] of its paths that
    /// are UTF-8, and the count of all of them.
    SourceDirty {
        paths: Vec<crate::api::dto::WorkspacePath>,
        total: u64,
    },
    /// The target's tree holds uncommitted work the fast-forward would overwrite, read the same
    /// way as [`Self::SourceDirty`].
    TargetDirty {
        paths: Vec<crate::api::dto::WorkspacePath>,
        total: u64,
    },
    /// A replayed commit conflicted; the rebase was rolled back and the workspace is at
    /// `rolled_back_to`, the head it had before the rebase.
    ReplayConflicted {
        rolled_back_to: crate::api::dto::GitOid,
    },
}

impl FenceRefusal {
    /// [`Self::SourceDirty`] or [`Self::TargetDirty`] over the paths a dirty reading answered.
    pub fn dirty(target: bool, paths: &[std::path::PathBuf]) -> Self {
        let total = u64::try_from(paths.len()).unwrap_or(u64::MAX);
        let paths = paths
            .iter()
            .filter_map(|path| crate::api::dto::WorkspacePath::new(path.as_path()).ok())
            .take(MAX_FENCE_PATHS)
            .collect();
        if target {
            Self::TargetDirty { paths, total }
        } else {
            Self::SourceDirty { paths, total }
        }
    }
}

impl CowshedError {
    pub fn new(code: ErrorCode, message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: hint.into(),
            other_build: None,
            recovering: None,
            healing: None,
            fence: None,
            retry: None,
            lifecycle_conflict: None,
        }
    }

    pub fn usage(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::Usage, message, hint)
    }

    pub fn not_found(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message, hint)
    }

    pub fn conflict(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, message, hint)
    }

    /// Preserve the structured CAS refusal while retaining the stable public error envelope.
    pub fn lifecycle_conflict(conflict: crate::storage::lifecycle::Conflict) -> Self {
        Self {
            code: ErrorCode::Conflict,
            message: conflict.to_string(),
            hint: "refresh workspace state and retry".to_owned(),
            other_build: None,
            recovering: None,
            healing: None,
            fence: None,
            retry: None,
            lifecycle_conflict: Some(Box::new(conflict)),
        }
    }

    /// The daemon's refusal of another build's request: a `Conflict` that names both builds in its
    /// sentence and carries them as [`OtherBuild`].
    pub fn other_build(other: OtherBuild) -> Self {
        let message = format!(
            "the cowshed daemon is build {}; this cowshed is build {}",
            other.daemon,
            other.caller.as_ref().map_or(
                "(unnamed)",
                crate::runtime::supervisor_socket::BuildId::as_str
            )
        );
        Self {
            code: ErrorCode::Conflict,
            message,
            hint: "run `cowshed gateway start` from the cowshed you mean to use".to_owned(),
            other_build: Some(Box::new(other)),
            recovering: None,
            healing: None,
            fence: None,
            retry: None,
            lifecycle_conflict: None,
        }
    }

    /// The daemon's refusal of `workspace` while it is still recovering the supervisor that
    /// served it before the daemon started: a `Conflict` carrying how many supervisors are left
    /// as [`cowshed_gateway_types::SupervisorRecovery`]. Every other workspace is served.
    pub fn recovering(
        recovery: cowshed_gateway_types::SupervisorRecovery,
        workspace: &crate::metadata::WorkspaceName,
    ) -> Self {
        Self {
            code: ErrorCode::Conflict,
            message: format!(
                "the cowshed daemon is still recovering workspace {workspace}'s supervisor from \
                 before it started ({} supervisors left to recover)",
                recovery.supervisors
            ),
            hint: "retry shortly; `cowshed gateway status` reports how many supervisors are left \
                   to recover"
                .to_owned(),
            other_build: None,
            recovering: Some(recovery),
            healing: None,
            fence: None,
            retry: None,
            lifecycle_conflict: None,
        }
    }

    /// The daemon's refusal of a request that depends on the mounts and sessions its startup
    /// pass is still restoring (05_gateway.md "Startup contract"): a `Conflict` carrying how far
    /// the pass has got as [`cowshed_gateway_types::StartupHeal`]. The daemon answers its status
    /// meanwhile, so the refusal names the command that reports progress.
    pub fn healing(heal: cowshed_gateway_types::StartupHeal) -> Self {
        Self {
            code: ErrorCode::Conflict,
            message: format!(
                "the cowshed gateway is still starting, {heal}, and this command needs the \
                 workspaces it is restoring"
            ),
            hint: "retry shortly; `cowshed gateway status` reports how far it has got".to_owned(),
            other_build: None,
            recovering: None,
            healing: Some(heal),
            fence: None,
            retry: None,
            lifecycle_conflict: None,
        }
    }

    /// A rebase or land refused at one of its fences: always a `Conflict`, carrying the reason
    /// and the observed value as [`FenceRefusal`].
    pub fn fence_refusal(
        fence: FenceRefusal,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self {
            fence: Some(Box::new(fence)),
            ..Self::conflict(message, hint)
        }
    }

    /// A refusal safe to retry unchanged: always a `Conflict`, naming why as [`Retry`].
    pub fn retryable(retry: Retry, message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            retry: Some(retry),
            ..Self::conflict(message, hint)
        }
    }

    pub fn environment_missing(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::EnvironmentMissing, message, hint)
    }

    pub fn sandbox_denied(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::SandboxDenied, message, hint)
    }

    pub fn integrity(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::new(ErrorCode::Integrity, message, hint)
    }

    /// An operation on cowshed's own storage failed; `hint` is the repair for a storage fault.
    ///
    /// EPERM anywhere in `source`'s chain is not a storage fault. Permission bits answer EACCES;
    /// the kernel's other EPERM sources are an immutable flag, unlinking another user's file in
    /// a sticky directory, and a system-protected path, and cowshed's own storage — store and
    /// mount-root directories the operator owns, never sticky, never flagged — has none of
    /// them. What remains is a sandbox refusing a path it withholds from the calling process:
    /// an agent harness shell, or a `cowshed exec` child calling cowshed again, typically
    /// allowed to open the store's existing lock files but not to create the temp file every
    /// durable publication starts with. The failing operation and its storage path are known
    /// here, which is what makes the errno authoritative enough for exit 6. The store is intact
    /// then, so "repair storage" would send the caller after a defect that does not exist: the
    /// move is to run the verb where the store is writable.
    ///
    /// A disk child that hung or failed (`mount_apfs`, `hdiutil`, `diskutil`), or a call that
    /// answered `ENFILE`, while the kernel vnode table is saturated is not a storage fault either:
    /// the report names the table and the limit to raise instead of "repair storage".
    pub fn storage_failure(
        message: impl Into<String>,
        source: &(dyn std::error::Error + 'static),
        hint: impl Into<String>,
    ) -> Self {
        Self::storage_failure_under(message, source, hint, crate::vnodes::VnodeTable::saturation)
    }

    /// [`Self::storage_failure`] with the vnode table read by `saturation`, which runs only when
    /// the chain holds a disk-child or `ENFILE` failure.
    fn storage_failure_under(
        message: impl Into<String>,
        source: &(dyn std::error::Error + 'static),
        hint: impl Into<String>,
        saturation: impl FnOnce() -> Option<crate::vnodes::VnodeTable>,
    ) -> Self {
        let mut starved = false;
        let mut cause = Some(source);
        while let Some(error) = cause {
            let errno = error
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error);
            if errno == Some(libc::EPERM) {
                return Self::sandbox_denied(
                    format!(
                        "{}: the process running cowshed is sandboxed away from cowshed's store",
                        message.into()
                    ),
                    "rerun the command from a shell whose sandbox allows writing the cowshed store",
                );
            }
            starved |= errno == Some(libc::ENFILE)
                || error.is::<crate::apfs::CommandRunError>()
                || matches!(
                    error.downcast_ref::<crate::apfs::ApfsError>(),
                    Some(
                        crate::apfs::ApfsError::CommandFailed { .. }
                            | crate::apfs::ApfsError::DiskImageHelperUnreachable(_)
                    )
                );
            cause = error.source();
        }
        if starved && let Some(table) = saturation() {
            // The saturation is the likely cause, not a proven one: the child's own failure stays
            // in the message, and the storage repair stays the next step if raising the limit
            // does not clear it.
            return Self::environment_missing(
                format!("{}; {table}", message.into()),
                format!(
                    "{}; if it recurs after that: {}",
                    table.remedy(),
                    hint.into()
                ),
            );
        }
        Self::environment_missing(message, hint)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message, "cowshed doctor --json")
    }

    /// The exact stale lifecycle fact, when this error came from `execute_checked`.
    pub fn lifecycle_conflict_source(&self) -> Option<&crate::storage::lifecycle::Conflict> {
        self.lifecycle_conflict.as_deref()
    }

    /// The two builds, when this is the daemon's refusal of another build's request.
    pub fn other_build_source(&self) -> Option<&OtherBuild> {
        self.other_build.as_deref()
    }

    /// What the daemon had left to recover, when this is its refusal of a workspace it was
    /// still recovering.
    pub const fn recovering_source(&self) -> Option<cowshed_gateway_types::SupervisorRecovery> {
        self.recovering
    }

    /// How far the daemon's startup pass had got, when this is its refusal of a request that
    /// depends on what that pass restores.
    pub const fn healing_source(&self) -> Option<cowshed_gateway_types::StartupHeal> {
        self.healing
    }

    /// The fence and what it observed, when this is a rebase's or land's fence refusal.
    pub fn fence_source(&self) -> Option<&FenceRefusal> {
        self.fence.as_deref()
    }

    /// Why retrying unchanged is the remedy, when this refusal is one a retry resolves.
    pub fn retry_source(&self) -> Option<Retry> {
        self.retry
    }

    pub const fn exit_code(&self) -> u8 {
        self.code.exit_code()
    }

    pub const fn exec_wrapper_exit_code(&self) -> u8 {
        self.code.exec_wrapper_exit_code()
    }
}

impl fmt::Display for CowshedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CowshedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.lifecycle_conflict
            .as_deref()
            .map(|conflict| conflict as &(dyn std::error::Error + 'static))
    }
}

pub type Result<T> = std::result::Result<T, CowshedError>;

#[cfg(test)]
mod tests {
    use super::{CowshedError, ErrorCode};

    const CODES: [ErrorCode; 7] = [
        ErrorCode::Internal,
        ErrorCode::Usage,
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::EnvironmentMissing,
        ErrorCode::SandboxDenied,
        ErrorCode::Integrity,
    ];

    #[test]
    fn stable_exit_codes_are_frozen() {
        assert_eq!(CODES.map(ErrorCode::exit_code), [1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            CODES.map(ErrorCode::exec_wrapper_exit_code),
            [100, 101, 102, 103, 104, 105, 106]
        );
    }

    #[test]
    fn cowshed_error_delegates_codes_and_displays_its_message() {
        let error = CowshedError::conflict("working tree changed", "retry adoption");

        assert_eq!(error.exit_code(), ErrorCode::Conflict.exit_code());
        assert_eq!(
            error.exec_wrapper_exit_code(),
            ErrorCode::Conflict.exec_wrapper_exit_code()
        );
        assert_eq!(error.to_string(), "working tree changed");
    }

    /// The fence crosses the controller socket as data: a consumer reads the reason and the
    /// observed value from the decoded error, and an error without one carries no `fence` key.
    #[test]
    fn a_fence_refusal_round_trips_the_wire_and_an_unfenced_error_has_no_fence_key() {
        use super::FenceRefusal;
        let observed = crate::api::dto::GitOid::new("2".repeat(40)).unwrap();
        let error = CowshedError::fence_refusal(
            FenceRefusal::NotFastForward {
                target_head: observed.clone(),
            },
            "main is at 2222, which raven is not based on",
            "cowshed rebase raven",
        );
        assert_eq!(error.code, ErrorCode::Conflict);
        let value = serde_json::to_value(&error).expect("error serializes");
        assert_eq!(
            value["fence"],
            serde_json::json!({ "reason": "notFastForward", "targetHead": observed })
        );
        let decoded: CowshedError = serde_json::from_value(value).expect("error decodes");
        assert_eq!(
            decoded.fence_source(),
            Some(&FenceRefusal::NotFastForward {
                target_head: observed
            })
        );

        let unfenced = serde_json::to_value(CowshedError::conflict("stale", "retry")).unwrap();
        assert!(unfenced.get("fence").is_none(), "{unfenced}");
        let decoded: CowshedError = serde_json::from_value(unfenced).unwrap();
        assert_eq!(decoded.fence_source(), None);

        let later: CowshedError = serde_json::from_value(serde_json::json!({
            "code": "conflict",
            "message": "a later build's fence",
            "hint": "retry",
            "fence": { "reason": "somethingNew", "observed": 1 },
        }))
        .expect("a later build's reason still decodes the error");
        assert_eq!(later.code, ErrorCode::Conflict);
        assert_eq!(later.message, "a later build's fence");
        assert_eq!(later.fence_source(), None);
    }

    /// A dirty tree can hold any number of paths, and the error must still fit one frame: the
    /// fence names a bounded prefix of the UTF-8 ones and counts all of them.
    #[test]
    fn a_dirty_fence_bounds_its_paths_and_counts_every_one() {
        use super::{FenceRefusal, MAX_FENCE_PATHS};
        use std::os::unix::ffi::OsStrExt;
        let mut paths = vec![std::path::PathBuf::from(std::ffi::OsStr::from_bytes(
            b"bad-\xff.txt",
        ))];
        paths.extend((0..MAX_FENCE_PATHS + 5).map(|index| format!("dir/{index}.rs").into()));
        let FenceRefusal::TargetDirty {
            paths: named,
            total,
        } = FenceRefusal::dirty(true, &paths)
        else {
            panic!("a target reading is TargetDirty");
        };
        assert_eq!(total, u64::try_from(MAX_FENCE_PATHS + 6).unwrap());
        assert_eq!(named.len(), MAX_FENCE_PATHS);
        assert_eq!(named[0].as_path(), std::path::Path::new("dir/0.rs"));
        assert!(matches!(
            FenceRefusal::dirty(false, &paths[..1]),
            FenceRefusal::SourceDirty { paths, total: 1 } if paths.is_empty()
        ));
    }

    #[test]
    fn json_uses_frozen_taxonomy_spelling() {
        let error = CowshedError::environment_missing("not adopted", "cowshed adopt");
        let value = serde_json::to_value(error).expect("error serializes");
        assert_eq!(value["code"], "environment-missing");
        assert_eq!(value["message"], "not adopted");
        assert_eq!(value["hint"], "cowshed adopt");
    }

    #[test]
    fn as_str_matches_serde_kebab_case_for_every_variant() {
        const SPELLINGS: [&str; 7] = [
            "internal",
            "usage",
            "not-found",
            "conflict",
            "environment-missing",
            "sandbox-denied",
            "integrity",
        ];
        assert_eq!(CODES.map(ErrorCode::as_str), SPELLINGS);
        for (code, spelling) in CODES.into_iter().zip(SPELLINGS) {
            let json = serde_json::to_value(code).expect("error code serializes");
            assert_eq!(json, spelling);
            let back: ErrorCode = serde_json::from_value(json).expect("error code deserializes");
            assert_eq!(back, code);
        }
    }

    #[test]
    fn a_sandbox_refusing_a_store_write_is_sandbox_denied_not_a_storage_fault() {
        let refused = std::io::Error::from_raw_os_error(libc::EPERM);
        let error =
            CowshedError::storage_failure("cannot persist journal", &refused, "repair storage");
        assert_eq!(error.code, ErrorCode::SandboxDenied);
        assert!(error.message.starts_with("cannot persist journal: "));
        assert!(
            error
                .hint
                .contains("sandbox allows writing the cowshed store")
        );

        // Found through a wrapping error's source chain, the way storage errors carry it.
        let wrapped = crate::metadata::MetadataError::Io {
            path: "/store/.journal.tmp.1".into(),
            source: std::io::Error::from_raw_os_error(libc::EPERM),
        };
        let error = CowshedError::storage_failure("cannot persist journal", &wrapped, "repair");
        assert_eq!(error.code, ErrorCode::SandboxDenied);
    }

    #[test]
    fn a_store_write_failing_for_any_other_reason_keeps_the_storage_repair() {
        for errno in [libc::EACCES, libc::ENOSPC, libc::ENOENT] {
            let failed = std::io::Error::from_raw_os_error(errno);
            let error =
                CowshedError::storage_failure("cannot persist journal", &failed, "repair storage");
            assert_eq!(error.code, ErrorCode::EnvironmentMissing, "errno {errno}");
            assert_eq!(error.message, "cannot persist journal");
            assert_eq!(error.hint, "repair storage");
        }
    }

    #[test]
    fn a_disk_child_starved_by_a_saturated_vnode_table_names_the_limit() {
        use crate::apfs::{ApfsError, CommandRequest, CommandRunError, CommandRunFailure};
        use crate::vnodes::VnodeTable;
        let saturated = || {
            Some(VnodeTable {
                allocated: 272_631,
                free: 0,
                limit: 263_168,
            })
        };
        let hung = || {
            ApfsError::from(CommandRunError {
                request: CommandRequest::new("/sbin/mount_apfs", ["-o", "nobrowse,owners"]),
                failure: CommandRunFailure::Deadline(std::time::Duration::from_secs(120)),
            })
        };

        let error =
            CowshedError::storage_failure_under("creating workspace", &hung(), "repair", saturated);
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert!(
            error.message.starts_with("creating workspace; "),
            "{error:?}"
        );
        assert!(error.message.contains("272631 vnodes in use"), "{error:?}");
        assert!(error.hint.contains("kern.maxvnodes=545262"), "{error:?}");

        // ENFILE is how a full table answers a call directly.
        let enfile = std::io::Error::from_raw_os_error(libc::ENFILE);
        let error = CowshedError::storage_failure_under("mount", &enfile, "repair", saturated);
        assert!(error.hint.contains("kern.maxvnodes"), "{error:?}");

        // The same hang on a host with room left is not blamed on the table.
        let error =
            CowshedError::storage_failure_under("creating workspace", &hung(), "repair", || None);
        assert_eq!(
            (error.message.as_str(), error.hint.as_str()),
            ("creating workspace", "repair")
        );

        // A saturated table does not claim failures no disk child or ENFILE produced.
        let full = std::io::Error::from_raw_os_error(libc::ENOSPC);
        let error = CowshedError::storage_failure_under("persist", &full, "repair", || {
            panic!("the table is read only for disk-child and ENFILE failures")
        });
        assert_eq!(error.hint, "repair");
    }

    #[test]
    fn integrity_is_a_typed_operational_failure() {
        let error = CowshedError::integrity(
            "sealed stdout digest does not match",
            "cowshed doctor --json",
        );
        let value = serde_json::to_value(&error).expect("error serializes");

        assert_eq!(error.exit_code(), 7);
        assert_eq!(error.exec_wrapper_exit_code(), 106);
        assert_eq!(value["code"], "integrity");
        assert_eq!(value["hint"], "cowshed doctor --json");
    }
}
