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
use cowshed_core::storage::bootstrap::{
    FstabOutcome, HostSetupPlan, HostSetupReport, HostUninstallPlan, UninstallFstabOutcome,
    UninstallReport, UninstallServiceOutcome, VolumeOutcome, VolumeRole, VolumeState,
    execute_host_setup, execute_host_uninstall, plan_host_setup, plan_host_uninstall,
};
use cowshed_core::metadata::ImageFormat;
use cowshed_core::storage::{StorageLayout, discover_session_images};
use cowshed_core::{CowshedError, NativeGatewayInventory, Result, validate_existing_host_storage};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

/// The exact sentence a run that may escalate prints before it escalates.
///
/// Fixed rather than derived: the caller has to be able to recognise "a dialog is about to appear"
/// without parsing the per-volume action list that follows it.
const AUTHORIZATION_ANNOUNCEMENT: &str =
    "setup will request administrator authorization to provision/remount cowshed volumes";

/// The teardown counterpart. Removing fstab pins edits a root-owned file, so it escalates for its
/// own reason and says so in its own words.
const UNINSTALL_AUTHORIZATION_ANNOUNCEMENT: &str =
    "setup --uninstall will request administrator authorization to remove cowshed's /etc/fstab pins";

/// What the volumes still hold, or why nobody could tell.
///
/// The second case is not a zero. A host whose store is merely unmounted looks empty to every
/// cheap check, and treating "cannot see" as "nothing there" is how a teardown quietly proceeds
/// over work someone still wanted. Both cases are the caller's to override; neither is guessed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkspaceCensus {
    Counted { projects: usize, workspaces: usize },
    Unknown { reason: String },
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
    /// Deactivate and delete the host services and the binaries they ran.
    async fn remove_host_services(&mut self) -> Result<Vec<HostArtifactRemoval>>;
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
            projects: projects.len(),
            workspaces,
        })
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
    if args.uninstall {
        return uninstall(setup, args.force, json, output).await;
    }
    repair(setup, json, output).await
}

async fn repair<S, W, E>(setup: &mut S, json: bool, output: &mut Output<W, E>) -> Result<i32>
where
    S: HostSetup,
    W: Write + Send,
    E: Write + Send,
{
    let plan = setup.plan().await?;
    announce(
        plan.requires_authorization,
        AUTHORIZATION_ANNOUNCEMENT,
        &plan.actions,
        output,
    )?;
    let report = setup.execute().await?;
    if json {
        output.success(report).map_err(output_error)?;
    } else {
        render_repair(&plan, &report, output)?;
    }
    // Exit 0 even when a volume was reported and not repaired: `setup` reports what it did, and
    // `doctor` owns the host's verdict (06_cli.md, "Onboarding and repair"). The unresolved state
    // is never silent — it is a row, a status line, and a field in the JSON report.
    output.hint("cowshed doctor").map_err(output_error)?;
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
    announce(
        plan.requires_authorization,
        UNINSTALL_AUTHORIZATION_ANNOUNCEMENT,
        &plan.pins_to_remove,
        output,
    )?;
    let removals = setup.remove_host_services().await?;
    let mut report = setup.execute_uninstall().await?;
    // Core owns the fstab half and reports it; the service half is this adapter's work, so the
    // adapter is what puts it in the report. Without this the JSON surface would be narrower than
    // the stderr surface, and a caller reading only the envelope would never learn that two
    // LaunchAgents and two binaries had been deleted.
    report.services = removals.iter().map(HostArtifactRemoval::to_wire).collect();
    if json {
        output.success(report).map_err(output_error)?;
    } else {
        render_uninstall(&census, &removals, &report, output)?;
    }
    output.hint("cowshed doctor").map_err(output_error)?;
    Ok(0)
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
            projects,
            workspaces,
        } => Some(CowshedError::conflict(
            format!(
                "{workspaces} {} still {} on this host's volumes across {projects} adopted {}; \
                 uninstall removes no volume and no image, so they would be left unmanaged",
                plural(*workspaces, "workspace", "workspaces"),
                if *workspaces == 1 { "exists" } else { "exist" },
                plural(*projects, "project", "projects"),
            ),
            "cowshed setup --uninstall --force",
        )),
        WorkspaceCensus::Unknown { reason } => Some(CowshedError::conflict(
            format!("could not establish what the volumes hold: {reason}"),
            "cowshed setup --uninstall --force",
        )),
    }
}

/// Everything the run will do, said before it does any of it.
fn announce<W: Write, E: Write>(
    requires_authorization: bool,
    announcement: &str,
    actions: &[String],
    output: &mut Output<W, E>,
) -> Result<()> {
    if requires_authorization {
        output.announce(announcement).map_err(output_error)?;
    }
    for action in actions {
        output
            .guidance(&format!("planned: {action}"))
            .map_err(output_error)?;
    }
    Ok(())
}

fn render_repair<W: Write, E: Write>(
    plan: &HostSetupPlan,
    report: &HostSetupReport,
    output: &mut Output<W, E>,
) -> Result<()> {
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
        .guidance(&repair_status(plan, report))
        .map_err(output_error)?;
    Ok(())
}

fn render_uninstall<W: Write, E: Write>(
    census: &WorkspaceCensus,
    removals: &[HostArtifactRemoval],
    report: &UninstallReport,
    output: &mut Output<W, E>,
) -> Result<()> {
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

/// `cowshed.store (store): absent -> provisioned`.
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
        VolumeState::MountedIncomplete => {
            String::from("mounted with a missing or wrong volume marker")
        }
        VolumeState::Detached => String::from("present but not mounted"),
        VolumeState::MisMounted { mounted_at } => {
            format!("mis-mounted at {}", mounted_at.display())
        }
        VolumeState::FoundElsewhere {
            container, device, ..
        } => format!("found outside this host's container (container {container}, device {device})"),
    }
}

/// The one state that needs a sentence of its own.
///
/// A `cowshed.store` in another container is not a missing volume and must never be reported as
/// one: repairing it would mean `deleteVolume`, so setup deliberately does nothing and says why.
/// The mount point is named when macOS has one, because that is where the bytes are readable now.
fn state_guidance(state: &VolumeState) -> Option<String> {
    match state {
        VolumeState::Absent
        | VolumeState::MountedValid
        | VolumeState::MountedIncomplete
        | VolumeState::Detached
        | VolumeState::MisMounted { .. } => None,
        VolumeState::FoundElsewhere {
            device, mounted_at, ..
        } => Some(match mounted_at {
            Some(path) => format!(
                "data is safe on {device}; not provisioned (readable at {})",
                path.display()
            ),
            None => format!("data is safe on {device}; not provisioned"),
        }),
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
        UninstallFstabOutcome::AlreadyClean => {
            String::from("/etc/fstab carried no cowshed pins")
        }
    }
}

/// The last line, and the one a healthy host is run for.
///
/// "Changed nothing" is read off the *plan*, not the per-volume action tokens: the plan is what
/// decided whether anything would happen, so a host with an empty plan and no escalation is the
/// exact definition of already set up. A volume left alone because it lives somewhere else is
/// reported separately, because claiming "everything already set up" over an unresolved finding
/// would be the comfortable answer rather than the true one.
fn repair_status(plan: &HostSetupPlan, report: &HostSetupReport) -> String {
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
    if plan.actions.is_empty() && !plan.requires_authorization {
        return String::from("everything already set up");
    }
    if report.authorized {
        return String::from("host storage is set up (one administrator authorization was used)");
    }
    String::from("host storage is set up")
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
        WorkspaceCensus::Counted { workspaces, .. } => format!(
            "cowshed's host presence is removed; {workspaces} {} and {} still on the volumes, which were not touched",
            plural(*workspaces, "workspace", "workspaces"),
            plural(*workspaces, "its image are", "their images are"),
        ),
        WorkspaceCensus::Unknown { .. } => String::from(
            "cowshed's host presence is removed; volume contents were never inspected and no volume was touched",
        ),
    }
}

const fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 { one } else { many }
}
