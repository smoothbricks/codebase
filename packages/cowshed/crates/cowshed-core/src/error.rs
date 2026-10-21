use std::fmt::{self, Write as _};

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
// The generated public error describes what this build emits, not its cross-build input decoder.
#[cfg_attr(any(), cowshed_api(output_only = "true"))]
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
    /// its startup pass has not finished restoring; absent from the wire otherwise. Boxed like
    /// `otherBuild`: the cause is rare, and unboxed its `Option` takes 16 bytes of every `Result`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    healing: Option<Box<cowshed_gateway_types::StartupHeal>>,
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
    /// Present only on a keyed exec that spawned nothing, or a refused read of a job id or
    /// admission key (07_api "Keyed admission and restart attachment" and "Captured-content
    /// authority"), naming why and either the job the key admitted or the set-aside store that
    /// may hold it. Absent from the wire otherwise, like `fence`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "known_to_this_build"
    )]
    admission: Option<Box<AdmissionRefusal>>,
    /// Present only on a job stdin write or close refused at the job's input, naming why and
    /// where that input stands. Absent from the wire otherwise, like `fence`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "known_to_this_build"
    )]
    stdin: Option<Box<StdinRefusal>>,
    /// Present only on an explicit removal refused after every gate that guards work passed,
    /// naming why. Absent from the wire otherwise, like `fence`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "known_to_this_build"
    )]
    removal: Option<Box<RemovalRefusal>>,

    /// One cold execution refusal, never two pointer slots in every Result.
    #[serde(flatten, default, deserialize_with = "known_to_this_build")]
    execution: Option<Box<ExecutionRefusal>>,
    /// Boxed like `otherBuild` and `fence`: the structured CAS refusal is rare, and every
    /// `Result` in cowshed carries this type.
    #[serde(skip)]
    lifecycle_conflict: Option<Box<crate::storage::lifecycle::Conflict>>,
}

/// Canonical wire fields remain flat; only the private storage groups these exclusive causes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
enum ExecutionRefusal {
    Admission {
        #[serde(rename = "cpuAdmission")]
        cpu_admission: CpuAdmissionRefusal,
    },
    Priority {
        priority: PriorityFailure,
    },
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

/// CPU admission is authority, not a class string in a child's environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CpuAdmissionRefusal {
    ClassNotPermitted {
        class: crate::api::dto::ExecClass,
        scope: crate::api::dto::ExecScope,
    },
    GatewayUnavailable {
        socket: std::path::PathBuf,
        error: String,
    },
    GatewayRefused {
        code: String,
        error: String,
    },
    /// A gateway restarted by an install cut this ask off and named that install; asked again
    /// once (05_gateway.md "A queued CPU ask outlives an install"), else returned as it is.
    CutOffByInstall {
        install: cowshed_gateway_types::InstallRestart,
        error: String,
    },
    /// The install that cut this ask off did not bring up a gateway that serves within its bound.
    InstallDidNotServe {
        install: cowshed_gateway_types::InstallRestart,
        waited_ms: u64,
    },
    Protocol {
        detail: String,
    },
}

/// What placing nice on the owned child observed; absent values were not observed, never guessed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityFailure {
    pub pid: u32,
    pub requested: i32,
    pub observed: Option<i32>,
    pub errno: Option<i32>,
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

/// A part of an exec request a repeat under its admission key must carry unchanged, by its
/// request name.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AdmissionField {
    Command,
    Cwd,
    Mode,
    Env,
    Stdin,
    Session,
    StdoutCopy,
    StderrCopy,
    Class,
    Want,
    Scope,
}

impl AdmissionField {
    /// The name the wire spells this field with, for sentences.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Cwd => "cwd",
            Self::Mode => "mode",
            Self::Env => "env",
            Self::Stdin => "stdin",
            Self::Session => "session",
            Self::StdoutCopy => "stdoutCopy",
            Self::StderrCopy => "stderrCopy",
            Self::Class => "class",
            Self::Want => "want",
            Self::Scope => "scope",
        }
    }
}

/// Why a keyed exec spawned nothing, or why a read of a job id or admission key was refused. A
/// `KeyConflict` or `StdinBound` names the job the key admitted, so the caller reaches that job
/// instead of exec'ing again. `Unprovable` names the unread set-aside store instead: the requested
/// job or key may be recorded there, so neither presence nor absence is known.
///
/// Fields are additive like [`FenceRefusal`]'s; a reason a later build added decodes as no
/// refusal (`CowshedError::admission_source` answers `None`) rather than losing the whole error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum AdmissionRefusal {
    /// The key admitted `job_id` for another request; `fields` names what this one changed. A
    /// `Conflict`.
    KeyConflict {
        job_id: crate::api::dto::JobId,
        fields: Vec<AdmissionField>,
    },
    /// The key admitted `job_id` with a live stdin stream, bound to that job once. The repeat
    /// answers the job and cannot bind a second stream: its bytes go through an attachment. A
    /// `Usage` error.
    StdinBound { job_id: crate::api::dto::JobId },
    /// A job store set aside at `set_aside` may hold the requested job id or admission key, and
    /// this build cannot read it: whether it recorded that job or admitted that key cannot be told.
    /// A keyed exec spawns nothing under the key, and a read by job id proves no absence. A
    /// `Conflict`.
    Unprovable { set_aside: std::path::PathBuf },
}

/// Why a job's stdin refused a write (07_api "Readiness and attachment stdin"). A write names the
/// byte of the job's input it starts at; `cursor` is always how many bytes of that input reached
/// the job's stdin pipe, which is where a writer resumes. Every variant is a `Conflict`; none is
/// safe to retry unchanged.
///
/// Fields are additive like [`FenceRefusal`]'s; a reason a later build added decodes as no
/// refusal (`CowshedError::stdin_source` answers `None`) rather than losing the whole error.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum StdinRefusal {
    /// The job's stdin has ended -- its attachment closed it, or the job's leader ended -- and
    /// takes no more bytes.
    Ended { cursor: u64 },
    /// The write at `offset` neither continues the input, whose next byte is `admitted`, nor
    /// repeats bytes the input already holds.
    Discontinuous {
        offset: u64,
        cursor: u64,
        admitted: u64,
    },
    /// The write at `offset` repeats bytes the input already holds, and carries other bytes.
    ReplayMismatch { offset: u64, cursor: u64 },
    /// The write at `offset` repeats bytes older than `retained_from`, the oldest the supervisor
    /// keeps, so nothing can prove them equal to what the job received.
    ReplayUnprovable {
        offset: u64,
        cursor: u64,
        retained_from: u64,
    },
    /// A write to the job's stdin pipe failed partway: how much of the input past `cursor`
    /// reached the job is unknown, so the stdin takes nothing more, whatever a writer resends.
    DeliveryUnknown { cursor: u64 },
}

/// Why an explicit removal refused a workspace that every gate guarding its work let go
/// (02_workspaces.md, `rm` step 1). Every variant is a `Conflict` that removed nothing.
///
/// Fields are additive like [`FenceRefusal`]'s; a reason a later build added decodes as no
/// refusal (`CowshedError::removal_source` answers `None`) rather than losing the whole error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RemovalRefusal {
    /// The workspace is younger than the age floor. `created_at` is the creation time its record
    /// carries, verbatim. `removable_at` is the first second at which the floor no longer refuses
    /// it. It is absent when that creation time cannot be read, and then the floor keeps refusing.
    Young {
        created_at: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        removable_at: Option<crate::api::dto::UtcTimestamp>,
    },
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
    pub fn class_not_permitted(
        class: crate::api::dto::ExecClass,
        scope: crate::api::dto::ExecScope,
    ) -> Self {
        Self::cpu_admission(CpuAdmissionRefusal::ClassNotPermitted { class, scope })
    }

    pub fn cpu_admission(refusal: CpuAdmissionRefusal) -> Self {
        let error = match &refusal {
            CpuAdmissionRefusal::ClassNotPermitted { class, scope } => Self::sandbox_denied(
                format!("class-not-permitted: worker exec cannot request {class:?} / {scope:?}"),
                "use the trusted coordinator/operator gate path; workers admit background, interactive and resident workspace work",
            ),
            CpuAdmissionRefusal::GatewayUnavailable { socket, error } => Self::environment_missing(
                format!(
                    "jobserver admission at {} is unavailable: {error}",
                    socket.display()
                ),
                "start the owning cowshed gateway; no unbudgeted spawn is permitted",
            ),
            CpuAdmissionRefusal::GatewayRefused { code, error } => Self::environment_missing(
                format!("jobserver admission refused ({code}): {error}"),
                "inspect cowshed gateway status and use the matching installed build",
            ),
            CpuAdmissionRefusal::CutOffByInstall { install, error } => Self::environment_missing(
                format!(
                    "jobserver admission refused (install-restart) by install {} ({}): {error}",
                    install.pid,
                    install.binary.display()
                ),
                "inspect cowshed gateway status; no unbudgeted spawn is permitted",
            ),
            CpuAdmissionRefusal::InstallDidNotServe { install, waited_ms } => {
                Self::environment_missing(
                    format!(
                        "the install {} ({}) restarting the gateway did not bring up one that serves within {} ms; waited {waited_ms} ms",
                        install.pid,
                        install.binary.display(),
                        install.serves_within_ms
                    ),
                    "inspect cowshed gateway status; no unbudgeted spawn is permitted",
                )
            }
            CpuAdmissionRefusal::Protocol { detail } => Self::environment_missing(
                format!("jobserver admission protocol failed: {detail}"),
                "inspect the owning gateway; no unbudgeted spawn is permitted",
            ),
        };
        Self {
            execution: Some(Box::new(ExecutionRefusal::Admission {
                cpu_admission: refusal,
            })),
            ..error
        }
    }

    pub fn priority(failure: PriorityFailure) -> Self {
        let error = Self::sandbox_denied(
            format!(
                "cannot place owned job pid {} at nice {}: observed {:?}, errno {:?}",
                failure.pid, failure.requested, failure.observed, failure.errno
            ),
            "interactive runs at the supervisor's own nice and every other class only lower (05_gateway.md \"Bands\"); never request an elevated priority",
        );
        Self {
            execution: Some(Box::new(ExecutionRefusal::Priority { priority: failure })),
            ..error
        }
    }

    pub fn cpu_admission_source(&self) -> Option<&CpuAdmissionRefusal> {
        match self.execution.as_deref() {
            Some(ExecutionRefusal::Admission { cpu_admission }) => Some(cpu_admission),
            Some(ExecutionRefusal::Priority { .. }) | None => None,
        }
    }
    pub fn priority_source(&self) -> Option<&PriorityFailure> {
        match self.execution.as_deref() {
            Some(ExecutionRefusal::Priority { priority }) => Some(priority),
            Some(ExecutionRefusal::Admission { .. }) | None => None,
        }
    }

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
            admission: None,
            stdin: None,
            removal: None,
            execution: None,
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
            admission: None,
            stdin: None,
            removal: None,
            execution: None,
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
            hint: "run `cowshed setup` from the cowshed you mean to use: it installs that build as both the daemon and the `cowshed` on PATH".to_owned(),
            other_build: Some(Box::new(other)),
            recovering: None,
            healing: None,
            fence: None,
            retry: None,
            admission: None,
            stdin: None,
            removal: None,
            execution: None,
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
            admission: None,
            stdin: None,
            removal: None,
            execution: None,
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
            healing: Some(Box::new(heal)),
            fence: None,
            retry: None,
            admission: None,
            stdin: None,
            removal: None,
            execution: None,
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

    /// A keyed exec that spawned nothing, or a refused read of a job id or admission key (07_api
    /// "Keyed admission and restart attachment" and "Captured-content authority"): a `Usage`
    /// error for a second stream bound to the key's job, else a `Conflict`, carrying the reason
    /// as [`AdmissionRefusal`].
    pub fn admission_refusal(refusal: AdmissionRefusal) -> Self {
        let error = match &refusal {
            AdmissionRefusal::KeyConflict { job_id, fields } => {
                const PREFIX: &str = "the admission key already admitted job ";
                const SUFFIX: &str = " for another request: this one changes its ";
                let names_bytes: usize = fields.iter().map(|field| field.as_str().len()).sum();
                let mut message = String::with_capacity(
                    PREFIX.len()
                        + 20
                        + SUFFIX.len()
                        + names_bytes
                        + 2 * fields.len().saturating_sub(1),
                );
                write!(message, "{PREFIX}{}{SUFFIX}", job_id.get())
                    .expect("formatting into a String cannot fail");
                for (index, field) in fields.iter().enumerate() {
                    if index != 0 {
                        message.push_str(", ");
                    }
                    message.push_str(field.as_str());
                }
                Self::conflict(
                    message,
                    format!(
                        "reach job {} with jobByKey, or exec the changed request under a new key",
                        job_id.get()
                    ),
                )
            }
            AdmissionRefusal::StdinBound { job_id } => Self::usage(
                format!(
                    "stdin already bound to job {}; attach to write",
                    job_id.get()
                ),
                format!(
                    "attach to job {} and write its stdin through the attachment",
                    job_id.get()
                ),
            ),
            AdmissionRefusal::Unprovable { set_aside } => Self::conflict(
                format!(
                    "the requested job or admission key may be recorded in the job history set \
                     aside at {}, which this cowshed cannot read",
                    set_aside.display()
                ),
                "retain the original job binding and inspect the unread history; whether the original job \
                 exists or ran is unknown, so re-executing it is not proven safe. A deliberately new \
                 exec may use another workspace incarnation or no admission key: that is a new effect, not \
                 a replay of the original. A read by job id proves no absence.",
            ),
        };
        Self {
            admission: Some(Box::new(refusal)),
            ..error
        }
    }

    /// A job stdin refusal: always a `Conflict`, carrying the reason and the input's cursor as
    /// [`StdinRefusal`].
    pub fn stdin_refusal(
        refusal: StdinRefusal,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self {
            stdin: Some(Box::new(refusal)),
            ..Self::conflict(message, hint)
        }
    }

    /// An explicit removal refused after every gate guarding its work passed: always a
    /// `Conflict`, carrying the reason as [`RemovalRefusal`].
    pub fn removal_refusal(
        refusal: RemovalRefusal,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self {
            removal: Some(Box::new(refusal)),
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
            if let Some(cowshed_apfs::ApfsError::AttachPoolExhausted(_)) =
                error.downcast_ref::<cowshed_apfs::ApfsError>()
            {
                // The host's finding, not storage's: no storage repair frees the attach pool.
                return Self::environment_missing(
                    message,
                    cowshed_apfs::attach_budget::EXHAUSTED_HINT,
                );
            }
            starved |= errno == Some(libc::ENFILE)
                || error.is::<cowshed_apfs::CommandRunError>()
                || matches!(
                    error.downcast_ref::<cowshed_apfs::ApfsError>(),
                    Some(
                        cowshed_apfs::ApfsError::CommandFailed { .. }
                            | cowshed_apfs::ApfsError::DiskImageHelperUnreachable(_)
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
    pub fn healing_source(&self) -> Option<cowshed_gateway_types::StartupHeal> {
        self.healing.as_deref().copied()
    }

    /// The fence and what it observed, when this is a rebase's or land's fence refusal.
    pub fn fence_source(&self) -> Option<&FenceRefusal> {
        self.fence.as_deref()
    }

    /// Why retrying unchanged is the remedy, when this refusal is one a retry resolves.
    pub fn retry_source(&self) -> Option<Retry> {
        self.retry
    }

    /// Why a keyed exec spawned nothing, or why a read of a job id or admission key was refused,
    /// when this is that refusal (07_api "Keyed admission and restart attachment" and
    /// "Captured-content authority").
    pub fn admission_source(&self) -> Option<&AdmissionRefusal> {
        self.admission.as_deref()
    }

    /// Why the job's stdin refused, when this is a stdin write's or close's refusal.
    pub fn stdin_source(&self) -> Option<StdinRefusal> {
        self.stdin.as_deref().copied()
    }

    /// Why an explicit removal refused a workspace its work gates let go, when this is that
    /// refusal.
    pub fn removal_source(&self) -> Option<&RemovalRefusal> {
        self.removal.as_deref()
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

    #[test]
    fn execution_causes_keep_their_flat_wire_and_unknown_sources_keep_the_envelope() {
        use super::{CpuAdmissionRefusal, PriorityFailure};
        let admission = CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: "fixture protocol".to_owned(),
        });
        let value = serde_json::to_value(&admission).expect("serialize admission");
        assert_eq!(
            value["cpuAdmission"],
            serde_json::json!({"reason":"protocol", "detail":"fixture protocol"})
        );
        assert!(value.get("execution").is_none());
        assert!(value.get("priority").is_none());
        let decoded: CowshedError = serde_json::from_value(value).expect("decode admission");
        assert_eq!(
            decoded.cpu_admission_source(),
            admission.cpu_admission_source()
        );
        assert!(decoded.priority_source().is_none());
        let priority = CowshedError::priority(PriorityFailure {
            pid: 7,
            requested: 10,
            observed: Some(0),
            errno: Some(libc::EPERM),
        });
        let value = serde_json::to_value(&priority).expect("serialize priority");
        assert_eq!(
            value["priority"],
            serde_json::json!({"pid":7, "requested":10, "observed":0, "errno":libc::EPERM})
        );
        assert!(value.get("cpuAdmission").is_none());
        let decoded: CowshedError = serde_json::from_value(value).expect("decode priority");
        assert_eq!(decoded.priority_source(), priority.priority_source());
        assert!(decoded.cpu_admission_source().is_none());
        for cause in [
            serde_json::json!({"cpuAdmission":{"reason":"futureRefusal"}}),
            serde_json::json!({"priority":{"future":true}}),
            serde_json::json!({}),
        ] {
            let mut value =
                serde_json::to_value(CowshedError::conflict("retained message", "retained hint"))
                    .unwrap();
            value
                .as_object_mut()
                .unwrap()
                .extend(cause.as_object().unwrap().clone());
            let decoded: CowshedError =
                serde_json::from_value(value).expect("unknown source keeps base envelope");
            assert_eq!(decoded.message, "retained message");
            assert_eq!(decoded.hint, "retained hint");
            assert!(decoded.cpu_admission_source().is_none());
            assert!(decoded.priority_source().is_none());
        }
    }

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

    /// The refusal crosses the controller socket as data: a caller reads the key's job from the
    /// decoded error and attaches to it, never exec'ing again.
    #[test]
    fn a_bound_stdin_refusal_is_a_usage_error_naming_the_job_on_the_wire() {
        use super::AdmissionRefusal;
        let job_id = crate::api::dto::JobId::new(7).unwrap();
        let error = CowshedError::admission_refusal(AdmissionRefusal::StdinBound { job_id });
        assert_eq!(error.code, ErrorCode::Usage);
        assert_eq!(
            error.message,
            "stdin already bound to job 7; attach to write"
        );
        let value = serde_json::to_value(&error).expect("error serializes");
        assert_eq!(
            value["admission"],
            serde_json::json!({ "reason": "stdinBound", "jobId": 7 })
        );
        let decoded: CowshedError = serde_json::from_value(value).expect("error decodes");
        assert_eq!(
            decoded.admission_source(),
            Some(&AdmissionRefusal::StdinBound { job_id })
        );
        let plain = serde_json::to_value(CowshedError::conflict("stale", "retry")).unwrap();
        assert!(plain.get("admission").is_none(), "{plain}");
    }

    #[test]
    fn a_key_conflict_names_each_changed_field_as_the_wire_spells_it() {
        use super::{AdmissionField, AdmissionRefusal};
        let fields = [
            AdmissionField::Command,
            AdmissionField::Cwd,
            AdmissionField::Mode,
            AdmissionField::Env,
            AdmissionField::Stdin,
            AdmissionField::Session,
            AdmissionField::StdoutCopy,
            AdmissionField::StderrCopy,
        ];
        for field in fields {
            assert_eq!(
                serde_json::to_value(field).unwrap(),
                serde_json::json!(field.as_str())
            );
        }
        let error = CowshedError::admission_refusal(AdmissionRefusal::KeyConflict {
            job_id: crate::api::dto::JobId::new(3).unwrap(),
            fields: vec![AdmissionField::Command, AdmissionField::StdoutCopy],
        });
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(
            error.message.ends_with("changes its command, stdoutCopy"),
            "{}",
            error.message
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
        use crate::vnodes::VnodeTable;
        use cowshed_apfs::{ApfsError, CommandRequest, CommandRunError, CommandRunFailure};
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
                failure: CommandRunFailure::Deadline {
                    deadline: std::time::Duration::from_secs(120),
                    pid: 4242,
                    ended: cowshed_apfs::ProcessStatus::Signal(libc::SIGKILL),
                },
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
    fn a_spent_attach_pool_sends_the_caller_to_doctor_not_storage_repair() {
        use cowshed_apfs::{
            ApfsError, AttachPoolFailure, CommandOutput, CommandRequest, ProcessStatus,
        };
        let spent = ApfsError::AttachPoolExhausted(Box::new(AttachPoolFailure {
            operation: "attach image without mounting",
            request: CommandRequest::new("/usr/sbin/diskutil", ["image", "attach"]),
            output: CommandOutput {
                status: ProcessStatus::Exit(1),
                stdout: Vec::new(),
                stderr: b"Error: Disk image operation failed with error code 150\n".to_vec(),
            },
        }));
        let error = CowshedError::storage_failure_under("build volume", &spent, "repair", || {
            panic!("a spent pool is not blamed on the vnode table")
        });
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert_eq!(error.hint, cowshed_apfs::attach_budget::EXHAUSTED_HINT);
        assert_eq!(error.message, "build volume");
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
