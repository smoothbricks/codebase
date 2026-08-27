//! Host storage provisioning, repair, and teardown: the `setup` verb.
//!
//! `setup` is the one command a stranded host can always run. It takes no project, because a host
//! whose volumes are absent has no adopted checkout to name, and it dispatches beside `gateway`
//! and `sccache` at the host-service layer rather than through the project runtime bridge
//! (01_storage.md, "`cowshed setup` owns this transaction").
//!
//! The verb is two phases on purpose. The plan is gathered *before* anything is executed so the
//! authorization announcement required by 06_cli.md rule 3 can be printed before the dialog
//! appears rather than alongside it; a plan with nothing to escalate raises no prompt at all.
//!
//! `--uninstall` runs the same transaction backwards and is deliberately narrower: it removes
//! cowshed's machine presence — the tagged `/etc/fstab` pins, the two LaunchAgents, the binaries
//! they ran — and no volume, image, or workspace. Everything it removes is rebuildable;
//! everything that holds data it leaves alone. That asymmetry is why teardown needs a census
//! rather than a confirmation prompt: cowshed has no prompts (06_cli.md), so the refusal is the
//! prompt and `--force` is the answer.

use crate::args::SetupArgs;
use crate::gateway_service::{
    canonical_home, gateway_launch_agent, output_error, remove_host_stable_executable,
    remove_launch_agent,
};
use crate::launchd::RemovalOutcome;
use crate::output::Output;
use crate::sccache_service::{remove_stale_socket, sccache_launch_agent};
use async_trait::async_trait;
use cowshed_core::api::EmptyResult;
use cowshed_core::metadata::ImageFormat;
use cowshed_core::storage::bootstrap::{
    FstabOutcome, HostAction, HostActionOutcome, HostActionResult, HostSetupPlan, HostSetupReport,
    HostUninstallPlan, UninstallFstabOutcome, UninstallReport, UninstallServiceOutcome,
    VolumeOutcome, VolumeRole, VolumeState, execute_host_setup, execute_host_uninstall,
    plan_host_setup, plan_host_uninstall,
};
use cowshed_core::storage::host_config::{
    AttachedWorkspace, HostConfigError, execute_mount_root_change, plan_mount_root_change,
};
use cowshed_core::storage::{StorageLayout, discover_session_images};
use cowshed_core::{
    CowshedError, ErrorCode, NativeGatewayInventory, Result, UnreachableMain,
    validate_existing_host_storage,
};
use std::fs;
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

/// The exact sentence a run that may escalate prints before it escalates.
///
/// Fixed rather than derived: the caller has to be able to recognise "a dialog is about to appear"
/// without reading the action list. The list that follows carries the intent, so this sentence
/// deliberately names only the prompt and points at them.
///
/// "provision" is cowshed's internal word for minting a volume and never appears in output a
/// person reads; the user-facing verb is "create".
const AUTHORIZATION_ANNOUNCEMENT: &str =
    "setup will request administrator authorization once, for the actions below";

/// The teardown counterpart. Removing fstab pins edits a root-owned file, so it escalates for its
/// own reason and says so in its own words.
const UNINSTALL_AUTHORIZATION_ANNOUNCEMENT: &str = "setup --uninstall will request administrator authorization to remove cowshed's /etc/fstab pins";

/// The promise a run can make when its plan mints nothing and removes nothing.
///
/// This is the sentence the common case needs most: a host whose volumes already exist and merely
/// lack boot pins is one authorization dialog away from being fixed, and the dialog itself gives a
/// person no way to tell a mount from a reformat. Printed only when the plan's own actions support
/// it, never as a default reassurance.
const NON_DESTRUCTIVE_PROMISE: &str =
    "no volumes will be created or deleted; existing data is untouched";

/// What the volumes still hold, or why nobody could tell.
///
/// The second case is not a zero. A host whose store is merely unmounted looks empty to every
/// cheap check, and treating "cannot see" as "nothing there" is how a teardown quietly proceeds
/// over work someone still wanted. Both cases are the caller's to override; neither is guessed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkspaceCensus {
    Counted {
        store: PathBuf,
        repo_ids: Vec<String>,
        workspaces: usize,
    },
    Unknown {
        reason: String,
    },
}

/// What setup could observe about the always-mounted mains.
///
/// Mirrors [`WorkspaceCensus`] deliberately, including its second case: "nobody could check" is
/// its own answer and must never render as "every main is mounted". Mains are always-mounted
/// (02_workspaces.md), so a host with one missing is not a host setup can call ready — but the
/// verdict itself belongs to `doctor`, so this only ever downgrades a sentence, never the exit
/// code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MainMounts {
    /// Every adopted project was checked; these are the ones whose main is not mounted.
    Checked(Vec<UnreachableMain>),
    Unknown {
        reason: String,
    },
}

/// One host artifact teardown touched, in the order it was touched.
///
/// Typed rather than stringly, so the prose rendering is an exhaustive match and a new outcome
/// cannot silently render as an old one. The stringly [`UninstallServiceOutcome`] is the wire
/// shape at the edge, produced from this and never the other way round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostArtifactRemoval {
    /// What it is, in words: `dev.cowshed.gateway agent`, `installed cowshed binary`. Frozen
    /// vocabulary: it is both the stderr label and the JSON `what`.
    pub what: String,
    pub outcome: RemovalOutcome,
}

impl HostArtifactRemoval {
    pub fn new(what: impl Into<String>, outcome: RemovalOutcome) -> Self {
        Self {
            what: what.into(),
            outcome,
        }
    }

    /// The wire form. `already-absent` is hyphenated to match core's action vocabulary
    /// (`already-current`, `marker-written`) even though the stderr prose reads "already absent".
    fn to_wire(&self) -> UninstallServiceOutcome {
        UninstallServiceOutcome {
            what: self.what.clone(),
            outcome: String::from(match self.outcome {
                RemovalOutcome::Removed => "removed",
                RemovalOutcome::AlreadyAbsent => "already-absent",
            }),
        }
    }
}

/// Everything `setup` does to a host, as the seam this verb is tested through.
///
/// Injectable because the real implementation provisions APFS volumes, edits `/etc/fstab`, and
/// drives launchd: the rendering and the refusal logic — which is all this module owns — have to
/// be provable without any of them.
#[async_trait]
pub trait HostSetup: Send {
    async fn plan(&mut self) -> Result<HostSetupPlan>;
    async fn execute(&mut self) -> Result<HostSetupReport>;
    async fn plan_uninstall(&mut self) -> Result<HostUninstallPlan>;
    async fn execute_uninstall(&mut self) -> Result<UninstallReport>;
    /// What the volumes hold right now, for the teardown refusal.
    async fn census(&mut self) -> Result<WorkspaceCensus>;
    /// Which adopted projects have no mounted main, for the readiness sentence.
    async fn unmounted_mains(&mut self) -> Result<MainMounts>;
    /// Deactivate and delete the host services and the binaries they ran.
    async fn remove_host_services(&mut self) -> Result<Vec<HostArtifactRemoval>>;
    /// Record the host session mount root. Refused while any session workspace is attached.
    async fn configure_mount_root(&mut self, mount_root: &Path) -> Result<PathBuf>;
}

/// The real host, rooted at the canonical home every other host verb resolves.
pub struct NativeHostSetup {
    home: PathBuf,
}

impl NativeHostSetup {
    pub fn for_canonical_home() -> Result<Self> {
        Ok(Self {
            home: canonical_home()?,
        })
    }
}

#[async_trait]
impl HostSetup for NativeHostSetup {
    async fn plan(&mut self) -> Result<HostSetupPlan> {
        plan_host_setup(&self.home).await
    }

    async fn execute(&mut self) -> Result<HostSetupReport> {
        execute_host_setup(&self.home).await
    }

    async fn plan_uninstall(&mut self) -> Result<HostUninstallPlan> {
        plan_host_uninstall(&self.home).await
    }

    async fn execute_uninstall(&mut self) -> Result<UninstallReport> {
        execute_host_uninstall(&self.home).await
    }

    /// Count what the store holds, or say why it could not be counted.
    ///
    /// Every failure along the way is [`WorkspaceCensus::Unknown`] rather than an error: "I cannot
    /// tell" is the honest answer to the occupancy question and the caller can still override it,
    /// whereas raising here would make an unmounted store an unremovable one.
    async fn census(&mut self) -> Result<WorkspaceCensus> {
        let storage = match validate_existing_host_storage(&self.home).await {
            Ok(storage) => storage,
            Err(error) => {
                return Ok(WorkspaceCensus::Unknown {
                    reason: error.message,
                });
            }
        };
        let inventory = NativeGatewayInventory::new(storage.clone());
        let projects = match inventory.adopted_projects().await {
            Ok(projects) => projects,
            Err(error) => {
                return Ok(WorkspaceCensus::Unknown {
                    reason: format!("could not enumerate adopted projects: {error}"),
                });
            }
        };
        let mut workspaces = 0;
        for project in &projects {
            let layout = match StorageLayout::new(storage.store(), &project.repo_id) {
                Ok(layout) => layout,
                Err(error) => {
                    return Ok(WorkspaceCensus::Unknown {
                        reason: format!(
                            "could not resolve the layout of {}: {error}",
                            project.repo_id
                        ),
                    });
                }
            };
            match count_project_workspaces(&layout) {
                Ok(count) => workspaces += count,
                Err(reason) => {
                    return Ok(WorkspaceCensus::Unknown {
                        reason: format!("{}: {reason}", project.repo_id),
                    });
                }
            }
        }
        Ok(WorkspaceCensus::Counted {
            store: storage.store().to_path_buf(),
            repo_ids: projects
                .iter()
                .map(|project| project.repo_id.to_string())
                .collect(),
            workspaces,
        })
    }

    /// Observe the always-mounted mains, or say why nobody could.
    ///
    /// Never an error, for the same reason the census is not: setup reports the host it found, and
    /// a store that cannot be enumerated must not turn a successful repair into a failed command.
    /// `doctor` is where an unmounted main becomes a verdict.
    async fn unmounted_mains(&mut self) -> Result<MainMounts> {
        let storage = match validate_existing_host_storage(&self.home).await {
            Ok(storage) => storage,
            Err(error) => {
                return Ok(MainMounts::Unknown {
                    reason: error.message,
                });
            }
        };
        match NativeGatewayInventory::new(storage).unmounted_mains().await {
            Ok(mains) => Ok(MainMounts::Checked(mains)),
            Err(error) => Ok(MainMounts::Unknown {
                reason: format!("could not check main workspace mounts: {error}"),
            }),
        }
    }

    /// Remove both agents, then both binaries.
    ///
    /// Order is load-bearing: the gateway agent is `KeepAlive`, so deleting the binary while its
    /// agent is still loaded leaves launchd respawning a path that no longer resolves.
    async fn remove_host_services(&mut self) -> Result<Vec<HostArtifactRemoval>> {
        let (gateway_binary, gateway_agent) = gateway_launch_agent(&self.home)?;
        let (sccache_binary, sccache_agent, sccache_socket) = sccache_launch_agent(&self.home)?;
        let removals = vec![
            HostArtifactRemoval::new(
                format!("{} agent", gateway_agent.label()),
                remove_launch_agent(&gateway_agent)?,
            ),
            HostArtifactRemoval::new(
                format!("{} agent", sccache_agent.label()),
                remove_launch_agent(&sccache_agent)?,
            ),
            HostArtifactRemoval::new(
                "installed cowshed binary",
                remove_host_stable_executable(&gateway_binary)?,
            ),
            HostArtifactRemoval::new(
                "installed sccache binary",
                remove_host_stable_executable(&sccache_binary)?,
            ),
        ];
        remove_stale_socket(&sccache_socket)?;
        Ok(removals)
    }

    async fn configure_mount_root(&mut self, mount_root: &Path) -> Result<PathBuf> {
        let storage = validate_existing_host_storage(&self.home).await?;
        let attached = NativeGatewayInventory::new(storage.clone())
            .all_attached()
            .await
            .map_err(|error| {
                CowshedError::integrity(
                    format!("could not list attached workspaces: {error}"),
                    "cowshed doctor --json",
                )
            })?
            .into_iter()
            .map(|fact| AttachedWorkspace::new(fact.repo_id, fact.workspace))
            .collect::<Vec<_>>();
        let plan = plan_mount_root_change(storage.store(), mount_root, attached)
            .map_err(host_config_error)?;
        let config = execute_mount_root_change(&plan).map_err(host_config_error)?;
        Ok(config.mount_root().to_path_buf())
    }
}

/// How many workspaces one project holds: its published session images plus its `main`.
///
/// Reads the store, never the mount table, because an image is a workspace whether or not it is
/// currently attached — a detached workspace is exactly the work a teardown would orphan. A
/// sessions directory that does not exist is a project with no sessions, not a failure.
fn count_project_workspaces(layout: &StorageLayout) -> std::result::Result<usize, String> {
    let sessions = &layout.project().sessions;
    let mut entries = Vec::new();
    match fs::read_dir(sessions) {
        Ok(listing) => {
            for entry in listing {
                entries.push(
                    entry
                        .map_err(|error| format!("could not read {}: {error}", sessions.display()))?
                        .path(),
                );
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("could not read {}: {error}", sessions.display())),
    }
    let mut workspaces = discover_session_images(entries)
        .map_err(|error| format!("could not enumerate session images: {error}"))?
        .len();
    for format in [ImageFormat::Asif, ImageFormat::Sparse] {
        if let Ok(image) = layout.main_image(format)
            && fs::symlink_metadata(image.image()).is_ok()
        {
            workspaces += 1;
        }
    }
    Ok(workspaces)
}

/// Resolve the real host and run the verb against it.
pub async fn dispatch_native<W, E>(
    args: &SetupArgs,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32>
where
    W: Write + Send,
    E: Write + Send,
{
    let mut host = NativeHostSetup::for_canonical_home()?;
    dispatch(&mut host, args, json, output).await
}

pub async fn dispatch<S, W, E>(
    setup: &mut S,
    args: &SetupArgs,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32>
where
    S: HostSetup,
    W: Write + Send,
    E: Write + Send,
{
    if let Some(mount_root) = args.mount_root.as_deref() {
        return set_mount_root(setup, mount_root, json, output).await;
    }
    if args.uninstall {
        return uninstall(setup, args.force, json, output).await;
    }
    repair(setup, json, output).await
}

async fn set_mount_root<S, W, E>(
    setup: &mut S,
    mount_root: &Path,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32>
where
    S: HostSetup,
    W: Write + Send,
    E: Write + Send,
{
    let path = setup.configure_mount_root(mount_root).await?;
    if json {
        output.success(EmptyResult {}).map_err(output_error)?;
    } else {
        output
            .bare_line(path.as_os_str().as_bytes())
            .map_err(output_error)?;
    }
    output
        .guidance(&format!("workspace mount root is {}", path.display()))
        .map_err(output_error)?;
    output.hint("cowshed doctor").map_err(output_error)?;
    Ok(0)
}

fn host_config_error(error: HostConfigError) -> CowshedError {
    match error {
        HostConfigError::WorkspacesAttached { workspaces } => {
            let names = workspaces
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            CowshedError::conflict(
                format!("workspace mount root cannot change while attached: {names}"),
                "detach every attached session, then cowshed setup --mount-root <dir>",
            )
        }
        HostConfigError::InvalidPath { path, reason } => CowshedError::usage(
            format!("invalid mount root {}: {reason}", path.display()),
            "cowshed setup --mount-root <dir>",
        ),
        other => CowshedError::environment_missing(other.to_string(), "cowshed setup"),
    }
}

async fn repair<S, W, E>(setup: &mut S, json: bool, output: &mut Output<W, E>) -> Result<i32>
where
    S: HostSetup,
    W: Write + Send,
    E: Write + Send,
{
    let plan = setup.plan().await?;
    announce_setup(&plan, output)?;
    let report = setup.execute().await.map_err(declined_authorization)?;
    // A run that stopped partway is a failure, and core reports it as a *successful report
    // carrying a failure* so the progress is not lost with the error. Both halves have to reach
    // the caller: the per-action rows say what happened, and the taxonomy says it did not work.
    // Exiting 0 here would tell every script the host was set up.
    let failure = report.failure().cloned();
    // Observed only on a run that finished. A run that stopped partway has its own headline and its
    // own remedy, and a main-mount observation on top of a failed caches mount would aim the reader
    // at the wrong problem.
    let mains = match &failure {
        Some(_) => None,
        None => Some(setup.unmounted_mains().await?),
    };
    if json {
        // The frozen envelope has no partial state, so a failed run answers `ok:false` and the
        // per-action detail goes to stderr — where progress belongs with `--json` anyway. Silently
        // answering `ok:true` over a failure is the one thing this must not do.
        match &failure {
            None => output.success(report).map_err(output_error)?,
            Some(_) => render_repair(&plan, &report, mains.as_ref(), output)?,
        }
    } else {
        render_repair(&plan, &report, mains.as_ref(), output)?;
    }
    if let Some(failure) = failure {
        return Err(partial_setup_failure(failure));
    }
    output.hint("cowshed doctor").map_err(output_error)?;
    // The gateway's startup pass is what mounts mains, so restarting it is the remedy — and it is
    // the remedy whether or not the volumes needed repairing, which is why this hint is not tied to
    // anything the plan did.
    if matches!(&mains, Some(MainMounts::Checked(unmounted)) if !unmounted.is_empty()) {
        output.hint("cowshed gateway start").map_err(output_error)?;
    }
    if matches!(
        setup.census().await?,
        WorkspaceCensus::Counted { workspaces: 0, .. }
    ) {
        output.hint("cowshed adopt").map_err(output_error)?;
    }
    Ok(0)
}

async fn uninstall<S, W, E>(
    setup: &mut S,
    force: bool,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32>
where
    S: HostSetup,
    W: Write + Send,
    E: Write + Send,
{
    // The census runs first and refuses first: nothing is removed while the answer to "is this
    // host still in use?" is yes or unknown.
    let census = setup.census().await?;
    if let Some(refusal) = refuse_occupied(&census, force) {
        return Err(refusal);
    }
    let plan = setup.plan_uninstall().await?;
    announce_uninstall(&plan, output)?;
    let removals = setup.remove_host_services().await?;
    let mut report = setup
        .execute_uninstall()
        .await
        .map_err(declined_authorization)?;
    // Core reports the system mount daemon it removed; append the per-user agents and binaries
    // owned by this adapter so both text and JSON describe the complete machine teardown.
    let system_removals = report.services.clone();
    report
        .services
        .extend(removals.iter().map(HostArtifactRemoval::to_wire));
    if json {
        output.success(report).map_err(output_error)?;
    } else {
        render_uninstall(&census, &system_removals, &removals, &report, output)?;
    }
    output.hint("cowshed doctor").map_err(output_error)?;
    Ok(0)
}

/// A person who dismissed the dialog denied cowshed a right; that is not a bug and not a
/// half-finished run.
///
/// Said in cowshed's words rather than the platform's, because `Authorization Services status
/// -60006` is not a sentence anyone can act on, and the actionable part — that the host is
/// unchanged — is not in it at all. Only an authoritatively typed denial is rewritten: 06_cli.md
/// permits exit 6 on denial evidence alone and never on scanning text, so every other failure
/// keeps its own taxonomy, message, and hint untouched.
fn declined_authorization(error: CowshedError) -> CowshedError {
    if error.code != ErrorCode::SandboxDenied {
        return error;
    }
    CowshedError::sandbox_denied(
        "administrator authorization was declined, so nothing on this host was changed",
        "cowshed setup",
    )
}

/// The failure that stopped the sequence, as this command's own outcome.
///
/// Core's taxonomy and hint are kept — it knows why the action failed and what fixes it — but a
/// denial noticed mid-sequence must not inherit [`declined_authorization`]'s sentence: earlier
/// actions had already succeeded, so "nothing on this host was changed" would be false. The state
/// of the host is stated once, by the status line above these rows, and never twice with two
/// different answers.
fn partial_setup_failure(failure: CowshedError) -> CowshedError {
    if failure.code != ErrorCode::SandboxDenied {
        return failure;
    }
    CowshedError::sandbox_denied(
        "administrator authorization was declined partway through the sequence above",
        "cowshed setup",
    )
}

/// The refusal that stands in for a confirmation prompt.
///
/// `conflict` rather than `usage`: the command line was well formed and the host said no. The hint
/// is the completed command line, per 06_cli.md's rule for missing confirmation flags.
fn refuse_occupied(census: &WorkspaceCensus, force: bool) -> Option<CowshedError> {
    if force {
        return None;
    }
    match census {
        WorkspaceCensus::Counted { workspaces: 0, .. } => None,
        WorkspaceCensus::Counted {
            store,
            repo_ids,
            workspaces,
        } => Some(CowshedError::conflict(
            format!(
                "{workspaces} {} still {} on {} across {}; \
                 uninstall removes no volume and no image, so they would be left unmanaged",
                plural(*workspaces, "workspace", "workspaces"),
                if *workspaces == 1 { "exists" } else { "exist" },
                store.display(),
                format_repo_ids(repo_ids),
            ),
            "cowshed setup --uninstall --force",
        )),
        WorkspaceCensus::Unknown { reason } => Some(CowshedError::conflict(
            format!("could not establish what the volumes hold: {reason}"),
            "mount first: cowshed setup",
        )),
    }
}

/// Everything the run will do to this host, said before it does any of it.
///
/// The disclosure is one block: the prompt, the safety promise when one can be made, then the
/// exact intent for each volume. When the run escalates, the whole block is unsuppressible — `-q`
/// hiding the list would leave "authorization once, for the actions below" pointing at nothing,
/// and a person cannot answer a dialog they were not told the reason for (06_cli.md rule 3). When
/// nothing escalates there is no dialog to answer, so the list is ordinary guidance.
fn announce_setup<W: Write, E: Write>(
    plan: &HostSetupPlan,
    output: &mut Output<W, E>,
) -> Result<()> {
    if plan.requires_authorization {
        output
            .announce(AUTHORIZATION_ANNOUNCEMENT)
            .map_err(output_error)?;
    }
    // Only worth saying when something is about to happen: on a healthy host there is no list for
    // it to reassure anyone about.
    if plan.non_destructive && !plan.actions.is_empty() {
        emit(plan.requires_authorization, NON_DESTRUCTIVE_PROMISE, output)?;
    }
    for action in &plan.actions {
        emit(plan.requires_authorization, &action_intent(action), output)?;
    }
    Ok(())
}

/// Teardown's disclosure: the prompt, then the exact fstab lines that will go.
fn announce_uninstall<W: Write, E: Write>(
    plan: &HostUninstallPlan,
    output: &mut Output<W, E>,
) -> Result<()> {
    if plan.requires_authorization {
        output
            .announce(UNINSTALL_AUTHORIZATION_ANNOUNCEMENT)
            .map_err(output_error)?;
    }
    for pin in &plan.pins_to_remove {
        emit(
            plan.requires_authorization,
            &format!("/etc/fstab pin will be removed: {pin}"),
            output,
        )?;
    }
    Ok(())
}

/// One disclosure line, unsuppressible exactly when it is explaining a dialog.
fn emit<W: Write, E: Write>(escalating: bool, line: &str, output: &mut Output<W, E>) -> Result<()> {
    if escalating {
        output.announce(line)
    } else {
        output.guidance(line)
    }
    .map_err(output_error)
}

/// The exact intent for one action, in the second person's terms rather than cowshed's.
///
/// Every volume arm names its identity and intended change; mount and repair arms also name the
/// destination because "will be mounted" without a path is not intent a person can consent to. No
/// default branch: an action core adds stops compiling here rather than being silently announced as
/// something it is not — and an unannounced action behind an authorization dialog is precisely the
/// contract violation rule 3 exists to prevent.
fn action_intent(action: &HostAction) -> String {
    match action {
        HostAction::CreateVolume {
            name,
            container,
            mount_at,
        } => format!(
            "{name} does not exist yet and will be created in container {container}, then mounted at {}",
            mount_at.display()
        ),
        HostAction::MountExisting {
            name,
            uuid,
            size_bytes,
            mount_at,
        } => format!(
            "{name} exists (UUID {uuid}, {}) and will be mounted at {}",
            decimal_size(*size_bytes),
            mount_at.display()
        ),
        HostAction::RepairMounted {
            name,
            uuid,
            size_bytes,
            mounted_at,
            mount_at,
        } => format!(
            "{name} exists (UUID {uuid}, {}) and is mounted at {}; it will be remounted at {}",
            decimal_size(*size_bytes),
            mounted_at.display(),
            mount_at.display()
        ),
        HostAction::EncryptVolume {
            name,
            uuid,
            size_bytes,
        } => format!(
            "{name} exists (UUID {uuid}, {}) and will be FileVault-encrypted in place; passphrase stored in System.keychain",
            decimal_size(*size_bytes)
        ),
        HostAction::PinFstab { uuid, mount_at } => format!(
            "/etc/fstab will pin UUID {uuid} at {} so it mounts at every boot",
            mount_at.display()
        ),
        HostAction::InstallMountService { label } => format!(
            "system LaunchDaemon {label} will be installed to unlock and mount cowshed volumes before login"
        ),
        // Named, not counted: 01_storage.md requires reclaimable stubs to be enumerated, because
        // "3 files will be deleted" is not something a person can agree to.
        HostAction::ReclaimStubs { paths } => format!(
            "these leftover placeholder files will be removed: {}",
            paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Volume sizes the way macOS states them: decimal units, one fraction digit.
///
/// Decimal rather than binary because that is what `diskutil` prints and what is printed on the
/// hardware; a person checking cowshed's sentence against Disk Utility has to see the same number.
/// Byte counts under a kilobyte keep no fraction — a fractional byte is not a quantity.
fn decimal_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    const STEP: f64 = 1000.0;
    if bytes < 1000 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= STEP && unit + 1 < UNITS.len() {
        value /= STEP;
        unit += 1;
    }
    // Re-promote when one-decimal rounding lands on the next unit, so 999_999_999_999 reads
    // "1.0 TB" and never "1000.0 GB".
    if (value * 10.0).round() / 10.0 >= STEP && unit + 1 < UNITS.len() {
        value /= STEP;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

fn render_repair<W: Write, E: Write>(
    plan: &HostSetupPlan,
    report: &HostSetupReport,
    mains: Option<&MainMounts>,
    output: &mut Output<W, E>,
) -> Result<()> {
    // The per-action outcomes come first and only when there is something to say: they are the
    // answer to "what actually happened to each thing you were told about", and on a run that
    // completed they would only repeat the volume rows below.
    if report.failure().is_some() {
        for outcome in &report.action_outcomes {
            output
                .guidance(&action_outcome_row(outcome))
                .map_err(output_error)?;
        }
    }
    for volume in &report.volumes {
        output.guidance(&volume_row(volume)).map_err(output_error)?;
        if let Some(guidance) = state_guidance(&volume.state_before) {
            output.guidance(&guidance).map_err(output_error)?;
        }
    }
    output
        .guidance(&fstab_phrase(&report.fstab))
        .map_err(output_error)?;
    output
        .guidance(&repair_status(plan, report, mains))
        .map_err(output_error)?;
    Ok(())
}

/// `cowshed.store exists (…) and will be mounted at …: failed — <why>`.
///
/// The intent sentence is reused verbatim rather than reworded in the past tense, so the line a
/// person consented to and the line reporting it are recognisably the same action. No default
/// branch: an outcome core adds stops compiling here rather than being reported as a success.
fn action_outcome_row(outcome: &HostActionOutcome) -> String {
    let intent = action_intent(&outcome.action);
    match &outcome.outcome {
        HostActionResult::Done => format!("{intent}: done"),
        HostActionResult::Failed { error } => format!("{intent}: FAILED — {}", error.message),
        // "not attempted" rather than "skipped": the sequence stopped, so this was never reached,
        // and "skipped" reads as a decision cowshed made about it.
        HostActionResult::Skipped => format!("{intent}: not attempted"),
    }
}

fn render_uninstall<W: Write, E: Write>(
    census: &WorkspaceCensus,
    system_removals: &[UninstallServiceOutcome],
    removals: &[HostArtifactRemoval],
    report: &UninstallReport,
    output: &mut Output<W, E>,
) -> Result<()> {
    for removal in system_removals {
        output
            .guidance(&format!(
                "{}: {}",
                removal.what,
                removal.outcome.replace('-', " ")
            ))
            .map_err(output_error)?;
    }
    for removal in removals {
        output
            .guidance(&format!(
                "{}: {}",
                removal.what,
                removal_word(removal.outcome)
            ))
            .map_err(output_error)?;
    }
    output
        .guidance(&uninstall_fstab_phrase(&report.fstab))
        .map_err(output_error)?;
    output
        .guidance(&uninstall_status(census))
        .map_err(output_error)?;
    Ok(())
}

const fn removal_word(outcome: RemovalOutcome) -> &'static str {
    match outcome {
        RemovalOutcome::Removed => "removed",
        RemovalOutcome::AlreadyAbsent => "already absent",
    }
}

/// `cowshed.store (store): absent -> created`.
///
/// Role as well as name because the name is a mutable label and the role is the identity
/// (01_storage.md, "Ownership, identity, and the volume label"): a hand-renamed volume still has
/// to read as the store.
fn volume_row(volume: &VolumeOutcome) -> String {
    format!(
        "{} ({}): {} -> {}",
        volume.name,
        role_word(volume.role),
        state_phrase(&volume.state_before),
        volume.action
    )
}

const fn role_word(role: VolumeRole) -> &'static str {
    match role {
        VolumeRole::Store => "store",
        VolumeRole::Caches => "caches",
        VolumeRole::Projects => "projects",
    }
}

/// The observed state, in words, with no default branch: a state core adds is a state that stops
/// compiling here rather than one that renders as something it is not.
fn state_phrase(state: &VolumeState) -> String {
    match state {
        VolumeState::Absent => String::from("absent"),
        VolumeState::MountedValid => String::from("mounted at its canonical path"),
        VolumeState::MountedIncomplete => String::from(
            "mounted, but its contents could not be identified as this host's cowshed volume",
        ),
        VolumeState::Detached => String::from("present but not mounted"),
        VolumeState::MisMounted { mounted_at } => {
            format!("mis-mounted at {}", mounted_at.display())
        }
        // Where the bytes are readable belongs in the row, beside the container and device that
        // identify them; the sentence that follows is reassurance and stays one clause long.
        VolumeState::FoundElsewhere {
            container,
            device,
            mounted_at,
        } => match mounted_at {
            Some(path) => format!(
                "found outside this host's container (container {container}, device {device}, mounted at {})",
                path.display()
            ),
            None => format!(
                "found outside this host's container (container {container}, device {device})"
            ),
        },
    }
}

/// The one state that needs a sentence of its own.
///
/// A `cowshed.store` in another container is not a missing volume and must never be reported as
/// one: adopting it would mean deleting a volume, so setup deliberately does nothing and says so.
/// One clause, because its whole job is reassurance — the container, device, and mount point are
/// already in the row above it.
fn state_guidance(state: &VolumeState) -> Option<String> {
    match state {
        VolumeState::Absent
        | VolumeState::MountedValid
        | VolumeState::MountedIncomplete
        | VolumeState::Detached
        | VolumeState::MisMounted { .. } => None,
        VolumeState::FoundElsewhere { device, .. } => Some(format!(
            "data is safe on {device}; cowshed left it untouched"
        )),
    }
}

fn fstab_phrase(fstab: &FstabOutcome) -> String {
    match fstab {
        FstabOutcome::Pinned => String::from("pinned the boot mounts in /etc/fstab"),
        FstabOutcome::AlreadyCurrent => String::from("/etc/fstab already pins the boot mounts"),
        FstabOutcome::Skipped(reason) => format!("/etc/fstab not pinned: {reason}"),
    }
}

fn uninstall_fstab_phrase(fstab: &UninstallFstabOutcome) -> String {
    match fstab {
        UninstallFstabOutcome::Removed => String::from("removed cowshed's /etc/fstab pins"),
        UninstallFstabOutcome::AlreadyClean => String::from("/etc/fstab carried no cowshed pins"),
    }
}

/// The last line, and the one a healthy host is run for.
///
/// Order matters: a run that stopped partway is reported as such before anything else, because
/// every other sentence here is a claim of completeness and would be false. Core reports partial
/// progress as a successful report carrying a failure rather than as an error, so this is the one
/// place the difference is visible to a person.
///
/// "Changed nothing" is read off the *plan*, not the per-volume action tokens: the plan is what
/// decided whether anything would happen, so a host with an empty plan and no escalation is the
/// exact definition of already set up. A volume left alone because it lives somewhere else is
/// reported separately, because claiming "everything already set up" over an unresolved finding
/// would be the comfortable answer rather than the true one.
///
/// The always-mounted mains qualify whichever readiness sentence was chosen rather than replacing
/// it: the volumes really are set up, and an unmounted main really is a host that is not serving
/// the user's own checkout (02_workspaces.md). It qualifies both healthy sentences because it
/// falsifies both — "everything already set up" over a missing checkout is the flattest lie of the
/// two, and is the branch a host with healthy volumes actually reaches. It never touches the
/// failure or FoundElsewhere sentences, which return above with headlines of their own.
fn repair_status(
    plan: &HostSetupPlan,
    report: &HostSetupReport,
    mains: Option<&MainMounts>,
) -> String {
    if report.failure().is_some() {
        let done = count_outcomes(report, |result| matches!(result, HostActionResult::Done));
        let failed = count_outcomes(report, |result| {
            matches!(result, HostActionResult::Failed { .. })
        });
        let not_attempted =
            count_outcomes(report, |result| matches!(result, HostActionResult::Skipped));
        return format!(
            "host storage is NOT set up: {done} {} done, {failed} failed, {not_attempted} not attempted",
            plural(done, "action", "actions"),
        );
    }
    let unresolved = report
        .volumes
        .iter()
        .filter(|volume| matches!(volume.state_before, VolumeState::FoundElsewhere { .. }))
        .count();
    if unresolved > 0 {
        return format!(
            "host storage is partially set up: {unresolved} {} outside this host's container and left untouched",
            plural(unresolved, "volume lives", "volumes live"),
        );
    }
    let ready = if plan.actions.is_empty() && !plan.requires_authorization {
        String::from("everything already set up")
    } else if report.authorized {
        String::from("host storage is set up (one administrator authorization was used)")
    } else {
        String::from("host storage is set up")
    };
    match mains {
        None => ready,
        Some(MainMounts::Checked(unmounted)) if unmounted.is_empty() => ready,
        Some(MainMounts::Checked(unmounted)) => format!(
            "{ready}, but {} main {} not mounted: {}",
            unmounted.len(),
            plural(unmounted.len(), "workspace is", "workspaces are"),
            unmounted
                .iter()
                .map(|main| main.repo_id.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        ),
        Some(MainMounts::Unknown { reason }) => {
            format!("{ready}; main workspace mounts could not be checked: {reason}")
        }
    }
}

fn count_outcomes(
    report: &HostSetupReport,
    predicate: impl Fn(&HostActionResult) -> bool,
) -> usize {
    report
        .action_outcomes
        .iter()
        .filter(|outcome| predicate(&outcome.outcome))
        .count()
}

/// What teardown left behind, said plainly.
///
/// Always names the data that survives, because "uninstalled" is exactly the word a caller would
/// otherwise read as "erased". The census is the count it was allowed to take, forced or not.
fn uninstall_status(census: &WorkspaceCensus) -> String {
    match census {
        WorkspaceCensus::Counted { workspaces: 0, .. } => String::from(
            "cowshed's host presence is removed; no workspaces existed and no volume was touched",
        ),
        WorkspaceCensus::Counted {
            store,
            repo_ids,
            workspaces,
        } => format!(
            "cowshed's host presence is removed; {workspaces} {} ({}) and their images are still on {}, which was not touched",
            plural(*workspaces, "workspace", "workspaces"),
            format_repo_ids(repo_ids),
            store.display(),
        ),
        WorkspaceCensus::Unknown { .. } => String::from(
            "cowshed's host presence is removed; volume contents were never inspected and no volume was touched",
        ),
    }
}

fn format_repo_ids(repo_ids: &[String]) -> String {
    if repo_ids.is_empty() {
        return String::from("no named repositories");
    }
    repo_ids.join(", ")
}

const fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 { one } else { many }
}
