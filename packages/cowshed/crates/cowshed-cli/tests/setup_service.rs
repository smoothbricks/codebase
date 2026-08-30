//! Contract tests for the `setup` verb.
//!
//! Everything asserted here is behaviour a caller can observe — parsed grammar, exact stdout and
//! stderr bytes, the frozen JSON envelope, the exit-shaping of a refusal, and the order in which
//! the host is touched. Nothing asserted here depends on how the arguments are parsed, so a
//! replacement parser is held to the same contract.

use async_trait::async_trait;
use cowshed_cli::args::{Command, GatewayCommand, ProjectDiscovery, SetupArgs, parse_args};
use cowshed_cli::gateway_service::ServiceBinaryRefresh;
use cowshed_cli::help;
use cowshed_cli::launchd::RemovalOutcome;
use cowshed_cli::output::Output;
use cowshed_cli::sccache_client_config::{
    ConfigChange, ConfigConflict, ConfigOutcome, ConfigReport,
};
use cowshed_cli::setup_service::{
    HostArtifactRemoval, HostSetup, MainMounts, WorkspaceCensus, dispatch as setup_dispatch,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::bootstrap::{
    FstabOutcome, HostAction, HostActionOutcome, HostActionResult, HostSetupPlan, HostSetupReport,
    HostUninstallPlan, UninstallFstabOutcome, UninstallReport, UninstallServiceOutcome,
    VolumeOutcome, VolumeRole, VolumeState,
};
use cowshed_core::{CowshedError, ErrorCode, Result, UnreachableMain};
use std::path::PathBuf;

/// A host whose every answer is canned, recording the order it was asked.
struct FakeHost {
    events: Vec<String>,
    plan: HostSetupPlan,
    report: HostSetupReport,
    uninstall_plan: HostUninstallPlan,
    uninstall_report: UninstallReport,
    census: WorkspaceCensus,
    removals: Vec<HostArtifactRemoval>,
    /// Which projects have no mounted main, for the readiness sentence.
    mains: MainMounts,
    /// What `setup` did to sccache's own config file, so the sentence it prints is provable
    /// without a real home directory or a real store.
    sccache: ConfigReport,
    /// What the escalating phase fails with, so the decline path is provable without a dialog.
    execute_error: Option<CowshedError>,
    mount_root_error: Option<CowshedError>,
    /// What reconciling the installed service binaries found, so the drift sentences are
    /// provable without launchd or a real installed copy.
    services: Vec<ServiceBinaryRefresh>,
}

/// A setup plan whose `non_destructive` is derived exactly the way core derives it — no
/// `CreateVolume`, and there are no delete actions — so the fake cannot express a plan core could
/// never produce.
fn setup_plan(actions: Vec<HostAction>, requires_authorization: bool) -> HostSetupPlan {
    let non_destructive = !actions
        .iter()
        .any(|action| matches!(action, HostAction::CreateVolume { .. }));
    HostSetupPlan {
        actions,
        volumes: Vec::new(),
        non_destructive,
        requires_authorization,
    }
}

fn empty_census() -> WorkspaceCensus {
    WorkspaceCensus::Counted {
        store: PathBuf::from("/private/cowshed/store"),
        repo_ids: Vec::new(),
        workspaces: 0,
    }
}

fn occupied_census() -> WorkspaceCensus {
    WorkspaceCensus::Counted {
        store: PathBuf::from("/private/cowshed/store"),
        repo_ids: vec![String::from("acme/api"), String::from("acme/web")],
        workspaces: 5,
    }
}

/// One project whose main is not mounted, named exactly as core names it.
fn detached_main() -> Vec<UnreachableMain> {
    vec![UnreachableMain {
        repo_id: RepoId::parse("acme/api").expect("repo"),
        image: PathBuf::from("/private/cowshed/store/acme/api/main.asif"),
        mountpoint: PathBuf::from("/Users/dev/src/api"),
        reason: String::from("main's volume is not mounted"),
    }]
}

impl Default for FakeHost {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            plan: setup_plan(Vec::new(), false),
            report: HostSetupReport {
                volumes: Vec::new(),
                fstab: FstabOutcome::AlreadyCurrent,
                authorized: false,
                action_outcomes: Vec::new(),
            },
            uninstall_plan: HostUninstallPlan {
                pins_to_remove: Vec::new(),
                requires_authorization: false,
            },
            uninstall_report: uninstall_report(UninstallFstabOutcome::AlreadyClean),
            census: empty_census(),
            removals: Vec::new(),
            mains: MainMounts::Checked(Vec::new()),
            sccache: ConfigReport {
                path: PathBuf::from(
                    "/Users/dev/Library/Application Support/Mozilla.sccache/config",
                ),
                store: PathBuf::from("/private/cowshed/caches/sccache"),
                outcome: ConfigOutcome::AlreadyCurrent,
            },
            execute_error: None,
            mount_root_error: None,
            services: Vec::new(),
        }
    }
}

#[async_trait]
impl HostSetup for FakeHost {
    async fn plan(&mut self) -> Result<HostSetupPlan> {
        self.events.push(String::from("plan"));
        Ok(self.plan.clone())
    }

    async fn execute(&mut self) -> Result<HostSetupReport> {
        self.events.push(String::from("execute"));
        match self.execute_error.take() {
            Some(error) => Err(error),
            None => Ok(self.report.clone()),
        }
    }

    async fn plan_uninstall(&mut self) -> Result<HostUninstallPlan> {
        self.events.push(String::from("plan-uninstall"));
        Ok(self.uninstall_plan.clone())
    }

    async fn execute_uninstall(&mut self) -> Result<UninstallReport> {
        self.events.push(String::from("execute-uninstall"));
        match self.execute_error.take() {
            Some(error) => Err(error),
            None => Ok(self.uninstall_report.clone()),
        }
    }

    async fn census(&mut self) -> Result<WorkspaceCensus> {
        self.events.push(String::from("census"));
        Ok(self.census.clone())
    }

    async fn unmounted_mains(&mut self) -> Result<MainMounts> {
        self.events.push(String::from("unmounted-mains"));
        Ok(self.mains.clone())
    }

    async fn remove_host_services(&mut self) -> Result<Vec<HostArtifactRemoval>> {
        self.events.push(String::from("remove-host-services"));
        Ok(self.removals.clone())
    }

    async fn refresh_host_services(&mut self) -> Result<Vec<ServiceBinaryRefresh>> {
        self.events.push(String::from("refresh-services"));
        Ok(self.services.clone())
    }

    async fn configure_sccache_client(&mut self) -> Result<ConfigReport> {
        self.events.push(String::from("configure-sccache-client"));
        Ok(self.sccache.clone())
    }

    async fn configure_mount_root(&mut self, mount_root: &std::path::Path) -> Result<PathBuf> {
        self.events
            .push(format!("configure-mount-root:{}", mount_root.display()));
        match self.mount_root_error.take() {
            Some(error) => Err(error),
            None => Ok(mount_root.to_path_buf()),
        }
    }
}

const REPAIR: SetupArgs = SetupArgs {
    uninstall: false,
    force: false,
    mount_root: None,
};
const UNINSTALL: SetupArgs = SetupArgs {
    uninstall: true,
    force: false,
    mount_root: None,
};
const FORCED_UNINSTALL: SetupArgs = SetupArgs {
    uninstall: true,
    force: true,
    mount_root: None,
};

struct Streams {
    stdout: String,
    stderr: String,
    exit: i32,
}

async fn run(host: &mut FakeHost, args: SetupArgs, json: bool, quiet: bool) -> Streams {
    let mut output = Output::new(Vec::new(), Vec::new(), quiet);
    let exit = setup_dispatch(host, &args, json, &mut output)
        .await
        .expect("dispatch succeeds");
    let (stdout, stderr) = output.into_inner();
    Streams {
        stdout: String::from_utf8(stdout).expect("utf8 stdout"),
        stderr: String::from_utf8(stderr).expect("utf8 stderr"),
        exit,
    }
}

/// A run that is expected to fail, keeping the streams: a partial run has to be judged on both
/// what it printed and how it exited, and asserting either alone would miss the point.
async fn failing_run(host: &mut FakeHost, args: SetupArgs, json: bool) -> (Streams, CowshedError) {
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = setup_dispatch(host, &args, json, &mut output)
        .await
        .expect_err("dispatch fails");
    let (stdout, stderr) = output.into_inner();
    (
        Streams {
            stdout: String::from_utf8(stdout).expect("utf8 stdout"),
            stderr: String::from_utf8(stderr).expect("utf8 stderr"),
            exit: i32::from(error.exit_code()),
        },
        error,
    )
}

async fn refusal(host: &mut FakeHost, args: SetupArgs) -> CowshedError {
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    setup_dispatch(host, &args, false, &mut output)
        .await
        .expect_err("dispatch refuses")
}

/// Core's own half of the teardown report. `services` is always empty here, because core returns
/// it empty and the adapter under test is what fills it — asserting that is the point.
fn uninstall_report(fstab: UninstallFstabOutcome) -> UninstallReport {
    UninstallReport {
        fstab,
        services: Vec::new(),
    }
}

fn volume(name: &str, role: VolumeRole, state: VolumeState, action: &str) -> VolumeOutcome {
    VolumeOutcome {
        name: String::from(name),
        role,
        state_before: state,
        action: String::from(action),
    }
}

/// The three actions a partial run walks: one succeeded, one failed, one was never reached.
fn interrupted_actions() -> (Vec<HostAction>, Vec<HostActionOutcome>) {
    let mount = HostAction::MountExisting {
        name: String::from("cowshed.store"),
        uuid: String::from("UUID-A"),
        size_bytes: 1_000_000_000_000,
        mount_at: PathBuf::from("/private/cowshed/store"),
    };
    let caches = HostAction::MountExisting {
        name: String::from("cowshed.caches"),
        uuid: String::from("UUID-B"),
        size_bytes: 2_000_000_000_000,
        mount_at: PathBuf::from("/private/cowshed/caches"),
    };
    let pin = HostAction::PinFstab {
        uuid: String::from("UUID-A"),
        mount_at: PathBuf::from("/private/cowshed/store"),
    };
    let outcomes = vec![
        HostActionOutcome {
            action: mount.clone(),
            outcome: HostActionResult::Done,
        },
        HostActionOutcome {
            action: caches.clone(),
            outcome: HostActionResult::Failed {
                error: CowshedError::environment_missing(
                    "cowshed.caches could not be mounted: resource busy",
                    "cowshed doctor",
                ),
            },
        },
        HostActionOutcome {
            action: pin.clone(),
            outcome: HostActionResult::Skipped,
        },
    ];
    (vec![mount, caches, pin], outcomes)
}

/// A run that stopped partway says so, action by action, and does not claim the host is set up.
///
/// Core reports partial progress as a successful report carrying a failure, so this is the case
/// where exiting 0 would tell every script the host was ready when it is not.
#[tokio::test]
async fn a_partial_run_reports_each_action_and_refuses_to_claim_success() {
    let (actions, action_outcomes) = interrupted_actions();
    let mut host = FakeHost {
        plan: setup_plan(actions, true),
        report: HostSetupReport {
            action_outcomes,
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Detached,
                "mounted",
            )],
            fstab: FstabOutcome::Skipped(String::from("cowshed.caches is not mounted")),
            authorized: true,
        },
        ..FakeHost::default()
    };

    let (streams, error) = failing_run(&mut host, REPAIR, false).await;

    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization once, for the actions below\n\
         cowshed: no volumes will be created or deleted; existing data is untouched\n\
         cowshed: cowshed.store exists (UUID UUID-A, 1.0 TB) and will be mounted at /private/cowshed/store\n\
         cowshed: cowshed.caches exists (UUID UUID-B, 2.0 TB) and will be mounted at /private/cowshed/caches\n\
         cowshed: /etc/fstab will pin UUID UUID-A at /private/cowshed/store so it mounts at every boot\n\
         cowshed: cowshed.store exists (UUID UUID-A, 1.0 TB) and will be mounted at /private/cowshed/store: done\n\
         cowshed: cowshed.caches exists (UUID UUID-B, 2.0 TB) and will be mounted at /private/cowshed/caches: FAILED — cowshed.caches could not be mounted: resource busy\n\
         cowshed: /etc/fstab will pin UUID UUID-A at /private/cowshed/store so it mounts at every boot: not attempted\n\
         cowshed: cowshed.store (store): present but not mounted -> mounted\n\
         cowshed: /etc/fstab not pinned: cowshed.caches is not mounted\n\
         cowshed: host storage is NOT set up: 1 action done, 1 failed, 1 not attempted\n"
    );
    // Never the completeness claims, and never the hint that follows a good run.
    assert!(!streams.stderr.contains("host storage is set up"));
    assert!(!streams.stderr.contains("everything already set up"));
    assert!(!streams.stderr.contains("next: cowshed doctor"));

    // Core's taxonomy and remedy survive: it knows why the action failed and what fixes it.
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    assert_eq!(streams.exit, 5);
    assert_eq!(
        error.message,
        "cowshed.caches could not be mounted: resource busy"
    );
    assert_eq!(error.hint, "cowshed doctor");
    // The census is never taken: there is no healthy host to inventory.
    assert_eq!(host.events, ["plan", "execute"]);
}

/// `--json` cannot answer `ok:true` over a failure. The frozen envelope has no partial state, so
/// the failure is the envelope and the per-action evidence goes to stderr.
#[tokio::test]
async fn a_partial_run_never_claims_success_in_json() {
    let (actions, action_outcomes) = interrupted_actions();
    let mut host = FakeHost {
        plan: setup_plan(actions, true),
        report: HostSetupReport {
            action_outcomes,
            volumes: Vec::new(),
            fstab: FstabOutcome::Skipped(String::from("cowshed.caches is not mounted")),
            authorized: true,
        },
        ..FakeHost::default()
    };

    let (streams, error) = failing_run(&mut host, REPAIR, true).await;

    assert_eq!(streams.stdout, "", "a failed run publishes no success body");
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    assert!(streams.stderr.contains(
        "cowshed: cowshed.caches exists (UUID UUID-B, 2.0 TB) and will be mounted at /private/cowshed/caches: FAILED — cowshed.caches could not be mounted: resource busy\n"
    ));
    assert!(streams.stderr.contains(
        "cowshed: host storage is NOT set up: 1 action done, 1 failed, 1 not attempted\n"
    ));
}

/// A denial noticed mid-sequence must not inherit the "nothing changed" sentence: earlier actions
/// had already succeeded, so that reassurance would be false. Still exit 6 — the evidence is the
/// same — and the state of the host is stated once, by the status line.
#[tokio::test]
async fn a_denial_partway_through_does_not_claim_nothing_changed() {
    let (actions, mut action_outcomes) = interrupted_actions();
    action_outcomes[1].outcome = HostActionResult::Failed {
        error: CowshedError::sandbox_denied(
            "execute privileged command failed with Authorization Services status -60006",
            "retry",
        ),
    };
    let mut host = FakeHost {
        plan: setup_plan(actions, true),
        report: HostSetupReport {
            action_outcomes,
            volumes: Vec::new(),
            fstab: FstabOutcome::Skipped(String::from("cowshed.caches is not mounted")),
            authorized: true,
        },
        ..FakeHost::default()
    };

    let (streams, error) = failing_run(&mut host, REPAIR, false).await;

    assert_eq!(streams.exit, 6);
    assert_eq!(
        error.message,
        "administrator authorization was declined partway through the sequence above"
    );
    assert!(
        !error.message.contains("nothing on this host was changed"),
        "an action had already succeeded, so nothing-changed would be a lie"
    );
    assert!(!error.message.contains("-60006"));
    assert!(streams.stderr.contains(
        "cowshed: host storage is NOT set up: 1 action done, 1 failed, 1 not attempted\n"
    ));
}

/// A run that completed prints no per-action rows: they would only repeat the volume rows, and the
/// evidence they exist for is what happened when things did *not* all happen.
#[tokio::test]
async fn a_completed_run_does_not_repeat_itself_action_by_action() {
    let (actions, _) = interrupted_actions();
    let done = actions
        .iter()
        .cloned()
        .map(|action| HostActionOutcome {
            action,
            outcome: HostActionResult::Done,
        })
        .collect();
    let mut host = FakeHost {
        plan: setup_plan(actions, true),
        report: HostSetupReport {
            action_outcomes: done,
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Detached,
                "mounted",
            )],
            fstab: FstabOutcome::Pinned,
            authorized: true,
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    assert!(!streams.stderr.contains(": done\n"));
    assert!(!streams.stderr.contains("NOT set up"));
    assert!(
        streams.stderr.contains(
            "cowshed: host storage is set up (one administrator authorization was used)\n"
        )
    );
}

/// The point of the verb: a healthy host is told it is healthy, in one line, and nothing else
/// happens. The plan is still gathered first, because that is what proves nothing is needed.
#[tokio::test]
async fn a_healthy_host_is_told_it_is_already_set_up() {
    let mut host = FakeHost {
        report: HostSetupReport {
            volumes: vec![
                volume(
                    "cowshed.store",
                    VolumeRole::Store,
                    VolumeState::MountedValid,
                    "already-current",
                ),
                volume(
                    "cowshed.caches",
                    VolumeRole::Caches,
                    VolumeState::MountedValid,
                    "already-current",
                ),
            ],
            fstab: FstabOutcome::AlreadyCurrent,
            authorized: false,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    assert_eq!(streams.stdout, "");
    assert_eq!(
        streams.stderr,
        "cowshed: cowshed.store (store): mounted at its canonical path -> already-current\n\
         cowshed: cowshed.caches (caches): mounted at its canonical path -> already-current\n\
         cowshed: /etc/fstab already pins the boot mounts\n\
         cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache\n\
         cowshed: everything already set up\n\
         next: cowshed adopt\n"
    );
    assert_eq!(
        host.events,
        [
            "plan",
            "execute",
            "unmounted-mains",
            "configure-sccache-client",
            "refresh-services",
            "census"
        ]
    );
}

/// The drift this exists for: a gateway kept running an installed binary from days before the
/// build being invoked, and `setup` answered "everything already set up" without touching it.
/// A refreshed service is reported, and the status line owns the fact instead of denying it.
#[tokio::test]
async fn a_refreshed_service_binary_is_reported_and_owns_the_status_line() {
    let mut host = FakeHost {
        services: vec![ServiceBinaryRefresh::Refreshed {
            service: String::from("dev.cowshed.gateway"),
        }],
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    let refreshed = streams
        .stderr
        .find("cowshed: dev.cowshed.gateway ran a stale binary; refreshed and restarted\n")
        .expect("the refresh is reported");
    let status = streams
        .stderr
        .find("cowshed: host services refreshed\n")
        .expect("the status line owns the refresh");
    assert!(refreshed < status);
    assert!(
        !streams.stderr.contains("everything already set up"),
        "a host that needed a service refresh was not already set up"
    );
}

/// A stale binary this run cannot durably refresh is still drift, and claiming readiness over it
/// would be the comfortable answer rather than the true one: the status line names the stale
/// service and the hint carries the remedy.
#[tokio::test]
async fn a_stale_service_binary_falsifies_the_ready_sentence_and_names_the_remedy() {
    let mut host = FakeHost {
        services: vec![ServiceBinaryRefresh::Stale {
            service: String::from("dev.cowshed.gateway"),
            remedy: String::from("run cowshed setup from a build outside every workspace mount"),
        }],
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    assert!(
        streams.stderr.contains(
            "cowshed: host storage is set up, but dev.cowshed.gateway runs a stale binary\n"
        ),
        "{}",
        streams.stderr
    );
    assert!(
        streams
            .stderr
            .contains("next: run cowshed setup from a build outside every workspace mount\n"),
        "{}",
        streams.stderr
    );
    assert!(!streams.stderr.contains("everything already set up"));
}

/// The defect this line exists for: an sccache client that inherited no cowshed environment
/// cached in its own private directory, the shared store served nothing, and no command said so.
/// `setup` writes sccache's own config file and names the file it wrote, so a reader who finds
/// that file later can tell who owns it.
#[tokio::test]
async fn a_written_sccache_config_names_the_file_and_the_store_it_points_at() {
    let mut host = FakeHost {
        sccache: ConfigReport {
            path: PathBuf::from("/Users/dev/Library/Application Support/Mozilla.sccache/config"),
            store: PathBuf::from("/private/cowshed/caches/sccache"),
            outcome: ConfigOutcome::Written(ConfigChange::Created),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    let written = streams
        .stderr
        .find("cowshed: wrote /Users/dev/Library/Application Support/Mozilla.sccache/config: an sccache client that inherited no cowshed environment now caches in /private/cowshed/caches/sccache\n")
        .expect("the file and the store it now names");
    // Before the status line, which is a claim about the whole host and is the last thing read.
    let status = streams
        .stderr
        .find("cowshed: everything already set up\n")
        .expect("status line");
    assert!(written < status);
}

/// A config cowshed did not write is not cowshed's to overwrite. The refusal names the file, what
/// it found there, and the store that will therefore not be shared — and it is a report rather
/// than a failure, because the host's storage really is set up.
#[tokio::test]
async fn a_foreign_sccache_config_is_named_and_left_alone_without_failing_setup() {
    let mut host = FakeHost {
        sccache: ConfigReport {
            path: PathBuf::from("/Users/dev/Library/Application Support/Mozilla.sccache/config"),
            store: PathBuf::from("/private/cowshed/caches/sccache"),
            outcome: ConfigOutcome::Refused(ConfigConflict::ForeignDirectory {
                found: String::from("/Users/dev/Library/Caches/Mozilla.sccache"),
            }),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(
        streams.exit, 0,
        "a file cowshed declines to overwrite is not a failed setup"
    );
    assert!(streams.stderr.contains(
        "cowshed: left /Users/dev/Library/Application Support/Mozilla.sccache/config alone: it already sets cache.disk.dir to /Users/dev/Library/Caches/Mozilla.sccache; a store-less sccache client will not share /private/cowshed/caches/sccache until cache.disk.dir names it\n"
    ));
}

/// Appending to somebody's own config says so, because the reassurance is the point: a person with
/// settings in that file has to know cowshed did not rewrite them.
#[tokio::test]
async fn an_appended_block_promises_the_rest_of_the_file_was_untouched() {
    let mut host = FakeHost {
        sccache: ConfigReport {
            path: PathBuf::from("/Users/dev/Library/Application Support/Mozilla.sccache/config"),
            store: PathBuf::from("/private/cowshed/caches/sccache"),
            outcome: ConfigOutcome::Written(ConfigChange::Appended),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(streams.stderr.contains(
        "cowshed: added cowshed's [cache.disk] block to /Users/dev/Library/Application Support/Mozilla.sccache/config, naming /private/cowshed/caches/sccache; every other setting in that file was left exactly as it was\n"
    ));
}

/// A host with no mounted caches volume gets no config at all: a file naming a directory beneath
/// an empty mountpoint would resolve onto the boot disk and become a fourth orphaned cache —
/// created by the command whose whole job is to prevent them.
#[tokio::test]
async fn no_config_is_written_when_there_is_no_shared_store_to_name() {
    let mut host = FakeHost {
        sccache: ConfigReport {
            path: PathBuf::from("/Users/dev/Library/Application Support/Mozilla.sccache/config"),
            store: PathBuf::from("/private/cowshed/caches/sccache"),
            outcome: ConfigOutcome::NoSharedStore {
                reason: String::from("cowshed.caches is not mounted"),
            },
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(streams.stderr.contains(
        "cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config not written: /private/cowshed/caches/sccache is not available (cowshed.caches is not mounted), and a config naming a cache that is not there would create a fourth one\n"
    ));
}

/// 06_cli.md rule 3: the sentence naming the prompt is printed *before* the phase that raises it.
/// The ordering is the assertion — the announcement has to precede `execute`, not accompany it.
#[tokio::test]
async fn an_escalating_run_announces_the_prompt_before_executing() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![
                HostAction::CreateVolume {
                    name: String::from("cowshed.store"),
                    container: String::from("disk3"),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                },
                HostAction::PinFstab {
                    uuid: String::from("1D6F0E1A-0000-4000-8000-00000000AAAA"),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                },
            ],
            true,
        ),
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Absent,
                "created",
            )],
            fstab: FstabOutcome::Pinned,
            authorized: true,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization once, for the actions below\n\
         cowshed: cowshed.store does not exist yet and will be created in container disk3, then mounted at /private/cowshed/store\n\
         cowshed: /etc/fstab will pin UUID 1D6F0E1A-0000-4000-8000-00000000AAAA at /private/cowshed/store so it mounts at every boot\n\
         cowshed: cowshed.store (store): absent -> created\n\
         cowshed: pinned the boot mounts in /etc/fstab\n\
         cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache\n\
         cowshed: host storage is set up (one administrator authorization was used)\n\
         next: cowshed adopt\n"
    );
    let announcement = streams
        .stderr
        .find("will request administrator authorization")
        .expect("the prompt is announced");
    let first_outcome = streams
        .stderr
        .find("(store): absent")
        .expect("the outcome is reported");
    assert!(
        announcement < first_outcome,
        "the announcement must precede the work"
    );
    assert_eq!(
        host.events,
        [
            "plan",
            "execute",
            "unmounted-mains",
            "configure-sccache-client",
            "refresh-services",
            "census"
        ]
    );
}

/// The user's actual host: volumes that already exist and valid, with no boot pins. Nothing is
/// created, so the run can promise it — and the promise is the whole point, because the macOS
/// dialog itself gives a person no way to tell a mount from a reformat.
///
/// This pins the sentence form parent mandated verbatim: name, UUID, size, and destination.
#[tokio::test]
async fn an_existing_volume_announces_its_identity_size_and_destination() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![
                HostAction::MountExisting {
                    name: String::from("cowshed.store"),
                    uuid: String::from("1D6F0E1A-0000-4000-8000-00000000AAAA"),
                    size_bytes: 1_000_000_000_000,
                    mount_at: PathBuf::from("/Users/dev/.cowshed"),
                },
                HostAction::PinFstab {
                    uuid: String::from("1D6F0E1A-0000-4000-8000-00000000AAAA"),
                    mount_at: PathBuf::from("/Users/dev/.cowshed"),
                },
            ],
            true,
        ),
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Detached,
                "mounted",
            )],
            fstab: FstabOutcome::Pinned,
            authorized: true,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization once, for the actions below\n\
         cowshed: no volumes will be created or deleted; existing data is untouched\n\
         cowshed: cowshed.store exists (UUID 1D6F0E1A-0000-4000-8000-00000000AAAA, 1.0 TB) and will be mounted at /Users/dev/.cowshed\n\
         cowshed: /etc/fstab will pin UUID 1D6F0E1A-0000-4000-8000-00000000AAAA at /Users/dev/.cowshed so it mounts at every boot\n\
         cowshed: cowshed.store (store): present but not mounted -> mounted\n\
         cowshed: pinned the boot mounts in /etc/fstab\n\
         cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache\n\
         cowshed: host storage is set up (one administrator authorization was used)\n\
         next: cowshed adopt\n"
    );
    assert!(!streams.stderr.contains("provision"));
}
/// Encryption is an in-place repair, but it changes the volume's security boundary. The
/// pre-authorization disclosure must name both FileVault and the durable passphrase location.
#[tokio::test]
async fn filevault_encryption_announces_in_place_change_and_system_keychain() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::EncryptVolume {
                name: String::from("cowshed.store"),
                uuid: String::from("1D6F0E1A-0000-4000-8000-00000000AAAA"),
                size_bytes: 1_000_000_000_000,
            }],
            true,
        ),
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(streams.stderr.contains(
        "cowshed: cowshed.store exists (UUID 1D6F0E1A-0000-4000-8000-00000000AAAA, 1.0 TB) and will be FileVault-encrypted in place; passphrase stored in System.keychain\n"
    ));
}

/// A plan that creates a volume cannot make the safety promise, and must not.
#[tokio::test]
async fn a_plan_that_creates_a_volume_makes_no_safety_promise() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::CreateVolume {
                name: String::from("cowshed.store"),
                container: String::from("disk3"),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            true,
        ),
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(
        !streams
            .stderr
            .contains("no volumes will be created or deleted")
    );
    assert!(streams.stderr.contains(
        "cowshed: cowshed.store does not exist yet and will be created in container disk3, then mounted at /private/cowshed/store\n"
    ));
}

/// A healthy host has no list, so it gets no promise about one either — the status line already
/// says everything is set up, and a reassurance about work nobody is doing is noise.
#[tokio::test]
async fn a_healthy_host_makes_no_safety_promise() {
    let mut host = FakeHost::default();

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(
        !streams
            .stderr
            .contains("no volumes will be created or deleted")
    );
    assert!(
        streams
            .stderr
            .contains("cowshed: everything already set up\n")
    );
}

/// Reclaimable stubs are named, never counted: "3 files will be deleted" is not something a
/// person can agree to (01_storage.md).
#[tokio::test]
async fn reclaimable_stubs_are_enumerated_by_name() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::ReclaimStubs {
                paths: vec![
                    PathBuf::from("/private/cowshed/store/.envrc"),
                    PathBuf::from("/private/cowshed/store/telemetry"),
                ],
            }],
            false,
        ),
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(streams.stderr.contains(
        "cowshed: these leftover placeholder files will be removed: /private/cowshed/store/.envrc, /private/cowshed/store/telemetry\n"
    ));
}

/// Sizes are decimal, one fraction digit, and promote rather than printing a four-digit mantissa —
/// a person comparing the sentence against Disk Utility has to read the same number.
#[tokio::test]
async fn volume_sizes_are_decimal_and_never_print_a_thousand_of_a_unit() {
    for (bytes, expected) in [
        (512_u64, "512 B"),
        (1_000, "1.0 KB"),
        (2_000_000_000_000, "2.0 TB"),
        (500_107_862_016, "500.1 GB"),
        // Rounds to 1000.0 GB at one decimal place, so it promotes instead.
        (999_999_999_999, "1.0 TB"),
    ] {
        let mut host = FakeHost {
            plan: setup_plan(
                vec![HostAction::MountExisting {
                    name: String::from("cowshed.store"),
                    uuid: String::from("U"),
                    size_bytes: bytes,
                    mount_at: PathBuf::from("/private/cowshed/store"),
                }],
                false,
            ),
            ..FakeHost::default()
        };

        let streams = run(&mut host, REPAIR, false, false).await;

        assert!(
            streams
                .stderr
                .contains(&format!("(UUID U, {expected}) and will be mounted at")),
            "{bytes} should render as {expected}, got:\n{}",
            streams.stderr
        );
    }
}

/// A run with nothing to escalate raises no prompt, so it says nothing about one.
#[tokio::test]
async fn a_run_that_cannot_escalate_never_mentions_authorization() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::RepairMounted {
                name: String::from("cowshed.caches"),
                uuid: String::from("1D6F0E1A-0000-4000-8000-00000000BBBB"),
                size_bytes: 2_000_000_000_000,
                mounted_at: PathBuf::from("/Volumes/cowshed.caches"),
                mount_at: PathBuf::from("/private/cowshed/caches"),
            }],
            false,
        ),
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.caches",
                VolumeRole::Caches,
                VolumeState::MisMounted {
                    mounted_at: PathBuf::from("/Volumes/cowshed.caches"),
                },
                "remounted",
            )],
            fstab: FstabOutcome::AlreadyCurrent,
            authorized: false,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(!streams.stderr.contains("authorization"));
    assert_eq!(
        streams.stderr,
        "cowshed: no volumes will be created or deleted; existing data is untouched\n\
         cowshed: cowshed.caches exists (UUID 1D6F0E1A-0000-4000-8000-00000000BBBB, 2.0 TB) and is mounted at /Volumes/cowshed.caches; it will be remounted at /private/cowshed/caches\n\
         cowshed: cowshed.caches (caches): mis-mounted at /Volumes/cowshed.caches -> remounted\n\
         cowshed: /etc/fstab already pins the boot mounts\n\
         cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache\n\
         cowshed: host storage is set up\n\
         next: cowshed adopt\n"
    );
}

/// A volume in another container is its own state with its own guidance, never "missing", and the
/// status line refuses to call the host set up while it stands. Adopting it would mean deleting a
/// volume, so the one thing this row must never imply is that setup could fix it.
#[tokio::test]
async fn a_volume_in_another_container_is_reported_and_left_alone() {
    let mut host = FakeHost {
        report: HostSetupReport {
            volumes: vec![
                volume(
                    "cowshed.store",
                    VolumeRole::Store,
                    VolumeState::FoundElsewhere {
                        container: String::from("disk4"),
                        device: String::from("disk4s7"),
                        mounted_at: Some(PathBuf::from("/Volumes/cowshed.store")),
                    },
                    "reported",
                ),
                volume(
                    "cowshed.caches",
                    VolumeRole::Caches,
                    VolumeState::FoundElsewhere {
                        container: String::from("disk4"),
                        device: String::from("disk4s8"),
                        mounted_at: None,
                    },
                    "reported",
                ),
            ],
            fstab: FstabOutcome::Skipped(String::from("no cowshed volume in the home container")),
            authorized: false,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(
        streams.stderr,
        "cowshed: cowshed.store (store): found outside this host's container (container disk4, device disk4s7, mounted at /Volumes/cowshed.store) -> reported\n\
         cowshed: data is safe on disk4s7; cowshed left it untouched\n\
         cowshed: cowshed.caches (caches): found outside this host's container (container disk4, device disk4s8) -> reported\n\
         cowshed: data is safe on disk4s8; cowshed left it untouched\n\
         cowshed: /etc/fstab not pinned: no cowshed volume in the home container\n\
         cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache\n\
         cowshed: host storage is partially set up: 2 volumes live outside this host's container and left untouched\n\
         next: cowshed adopt\n"
    );
    assert!(!streams.stderr.contains("absent"));
    assert!(!streams.stderr.contains("everything already set up"));
    assert!(
        !streams.stderr.contains("provision"),
        "`provision` is cowshed's internal word and must never reach a person"
    );
}

/// `--json` puts exactly one frozen envelope on stdout and keeps prose off it. The rendered rows
/// are the human view of the same report, so they are not repeated on stderr.
#[tokio::test]
async fn json_emits_one_frozen_envelope_and_no_prose_on_stdout() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::CreateVolume {
                name: String::from("cowshed.store"),
                container: String::from("disk3"),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            true,
        ),
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::FoundElsewhere {
                    container: String::from("disk4"),
                    device: String::from("disk4s7"),
                    mounted_at: None,
                },
                "reported",
            )],
            fstab: FstabOutcome::Pinned,
            authorized: true,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, true, false).await;

    assert_eq!(
        streams.stdout,
        "{\"ok\":true,\"result\":{\"actionOutcomes\":[],\"volumes\":[{\"name\":\"cowshed.store\",\"role\":\"store\",\
         \"stateBefore\":{\"foundElsewhere\":{\"container\":\"disk4\",\"device\":\"disk4s7\",\
         \"mountedAt\":null}},\"action\":\"reported\"}],\"fstab\":\"pinned\",\"authorized\":true}}\n"
    );
    // The announcement is guidance, not an answer: it stays on stderr even in JSON mode, because a
    // dialog is about to appear either way.
    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization once, for the actions below\n\
         cowshed: cowshed.store does not exist yet and will be created in container disk3, then mounted at /private/cowshed/store\n\
         cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache\n\
         next: cowshed adopt\n"
    );
}

/// `-q` drops guidance and keeps the hint — and keeps the authorization announcement, which is not
/// guidance a caller can opt out of.
#[tokio::test]
async fn quiet_suppresses_rows_but_never_the_prompt_announcement_or_the_hint() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::CreateVolume {
                name: String::from("cowshed.store"),
                container: String::from("disk3"),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            true,
        ),
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Absent,
                "created",
            )],
            fstab: FstabOutcome::Pinned,
            authorized: true,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, true).await;

    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization once, for the actions below\n\
         cowshed: cowshed.store does not exist yet and will be created in container disk3, then mounted at /private/cowshed/store\n\
         next: cowshed adopt\n"
    );
    assert_eq!(streams.stdout, "");
}

/// Dismissing the dialog denies cowshed a right: exit 6, cowshed's own sentence, and an explicit
/// promise that the host is unchanged.
///
/// The platform's `Authorization Services status -60006` is deliberately not surfaced — it is not
/// a sentence anyone can act on, and the part that matters (nothing changed) is not in it.
#[tokio::test]
async fn a_declined_authorization_is_policy_denied_and_exits_six() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::CreateVolume {
                name: String::from("cowshed.store"),
                container: String::from("disk3"),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            true,
        ),
        execute_error: Some(CowshedError::sandbox_denied(
            "execute privileged command failed with Authorization Services status -60006",
            "retry",
        )),
        ..FakeHost::default()
    };

    let error = refusal(&mut host, REPAIR).await;

    assert_eq!(error.code, ErrorCode::SandboxDenied);
    assert_eq!(error.exit_code(), 6);
    assert_eq!(
        error.message,
        "administrator authorization was declined, so nothing on this host was changed"
    );
    assert_eq!(error.hint, "cowshed setup");
    assert!(!error.message.contains("-60006"));
    // The announcement still happened: the caller was told a dialog was coming before it came,
    // which is the whole point of gathering the plan first.
    assert_eq!(host.events, ["plan", "execute"]);
}

/// Teardown escalates too — it edits a root-owned file — so a decline there is the same answer.
#[tokio::test]
async fn a_declined_uninstall_authorization_is_policy_denied_and_exits_six() {
    let mut host = FakeHost {
        uninstall_plan: HostUninstallPlan {
            pins_to_remove: vec![String::from("UUID=1111 /private/cowshed/store")],
            requires_authorization: true,
        },
        execute_error: Some(CowshedError::sandbox_denied(
            "execute privileged command failed with Authorization Services status -60005",
            "retry",
        )),
        ..FakeHost::default()
    };

    let error = refusal(&mut host, UNINSTALL).await;

    assert_eq!(error.exit_code(), 6);
    assert_eq!(
        error.message,
        "administrator authorization was declined, so nothing on this host was changed"
    );
}

/// Only an authoritatively typed denial becomes exit 6. 06_cli.md forbids inferring a denial from
/// text, so a genuine failure that merely mentions authorization keeps its own taxonomy, message,
/// and hint — otherwise a broken `diskutil` would be reported to the user as "you said no".
#[tokio::test]
async fn a_failure_that_is_not_a_denial_keeps_its_own_taxonomy() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::CreateVolume {
                name: String::from("cowshed.store"),
                container: String::from("disk3"),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            true,
        ),
        execute_error: Some(CowshedError::internal(
            "authorization session lock is poisoned",
        )),
        ..FakeHost::default()
    };

    let error = refusal(&mut host, REPAIR).await;

    assert_eq!(error.code, ErrorCode::Internal);
    assert_eq!(error.exit_code(), 1);
    assert_eq!(error.message, "authorization session lock is poisoned");
}

/// Teardown removes the machine presence and says, every time, that the data is still there.
#[tokio::test]
async fn uninstall_removes_host_presence_and_names_what_survives() {
    let mut host = FakeHost {
        uninstall_plan: HostUninstallPlan {
            pins_to_remove: vec![String::from("UUID=1111 /private/cowshed/store")],
            requires_authorization: true,
        },
        uninstall_report: uninstall_report(UninstallFstabOutcome::Removed),
        census: empty_census(),
        removals: vec![
            HostArtifactRemoval::new("dev.cowshed.gateway agent", RemovalOutcome::Removed),
            HostArtifactRemoval::new("dev.cowshed.sccache agent", RemovalOutcome::AlreadyAbsent),
            HostArtifactRemoval::new("installed cowshed binary", RemovalOutcome::Removed),
        ],
        ..FakeHost::default()
    };

    let streams = run(&mut host, UNINSTALL, false, false).await;

    assert_eq!(streams.exit, 0);
    assert_eq!(streams.stdout, "");
    assert_eq!(
        streams.stderr,
        "cowshed: setup --uninstall will request administrator authorization to remove cowshed's /etc/fstab pins\n\
         cowshed: /etc/fstab pin will be removed: UUID=1111 /private/cowshed/store\n\
         cowshed: dev.cowshed.gateway agent: removed\n\
         cowshed: dev.cowshed.sccache agent: already absent\n\
         cowshed: installed cowshed binary: removed\n\
         cowshed: removed cowshed's /etc/fstab pins\n\
         cowshed: cowshed's host presence is removed; no workspaces existed and no volume was touched\n"
    );
    // The census is taken before anything is removed: a refusal must cost the host nothing.
    assert_eq!(
        host.events,
        [
            "census",
            "plan-uninstall",
            "remove-host-services",
            "execute-uninstall"
        ]
    );
}

/// Uninstall removes no volume, so workspaces still on those volumes would be left unmanaged. That
/// is a conflict the caller confirms, and the refusal costs the host nothing.
#[tokio::test]
async fn uninstall_refuses_occupied_volumes_until_forced() {
    let occupied = || FakeHost {
        census: occupied_census(),
        uninstall_report: uninstall_report(UninstallFstabOutcome::Removed),
        ..FakeHost::default()
    };

    let mut host = occupied();
    let error = refusal(&mut host, UNINSTALL).await;
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(
        error.message,
        "5 workspaces still exist on /private/cowshed/store across acme/api, acme/web; \
         uninstall removes no volume and no image, so they would be left unmanaged"
    );
    assert_eq!(error.hint, "cowshed setup --uninstall --force");
    assert_eq!(host.events, ["census"], "a refusal touches nothing");

    let mut host = occupied();
    let streams = run(&mut host, FORCED_UNINSTALL, false, false).await;
    assert!(streams.stderr.contains(
        "cowshed: cowshed's host presence is removed; 5 workspaces (acme/api, acme/web) and their images are still on /private/cowshed/store, which was not touched\n"
    ));
    assert_eq!(
        host.events,
        [
            "census",
            "plan-uninstall",
            "remove-host-services",
            "execute-uninstall"
        ]
    );
}

/// An unmounted store looks empty to every cheap check. Treating "cannot see" as "nothing there"
/// is how a teardown quietly proceeds over work someone still wanted, so it refuses instead.
#[tokio::test]
async fn uninstall_refuses_when_occupancy_cannot_be_established() {
    let unknown = || FakeHost {
        census: WorkspaceCensus::Unknown {
            reason: String::from("cowshed.store is not mounted at /private/cowshed/store"),
        },
        ..FakeHost::default()
    };

    let mut host = unknown();
    let error = refusal(&mut host, UNINSTALL).await;
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(
        error.message,
        "could not establish what the volumes hold: cowshed.store is not mounted at /private/cowshed/store"
    );
    assert_eq!(error.hint, "mount first: cowshed setup");

    let mut host = unknown();
    let streams = run(&mut host, FORCED_UNINSTALL, false, false).await;
    assert!(streams.stderr.contains(
        "cowshed: cowshed's host presence is removed; volume contents were never inspected and no volume was touched\n"
    ));
}

/// Teardown's `--json` carries the whole outcome, not just either layer's half.
///
/// Core removes the system mount daemon and System.keychain items before the adapter reports the
/// per-user agents and binaries. Order matters: the machine remounter first, then its credentials,
/// then both agents, then both binaries. The gateway agent is `KeepAlive`, so deleting its binary
/// under a loaded agent would leave launchd respawning a vanished path.
#[tokio::test]
async fn uninstall_json_reports_the_services_the_adapter_removed() {
    let mut host = FakeHost {
        uninstall_report: UninstallReport {
            fstab: UninstallFstabOutcome::Removed,
            services: vec![
                UninstallServiceOutcome {
                    what: String::from("dev.cowshed.storage system LaunchDaemon"),
                    outcome: String::from("removed"),
                },
                UninstallServiceOutcome {
                    what: String::from("cowshed.store System.keychain item"),
                    outcome: String::from("removed"),
                },
                UninstallServiceOutcome {
                    what: String::from("cowshed.caches System.keychain item"),
                    outcome: String::from("already-absent"),
                },
            ],
        },
        removals: vec![
            HostArtifactRemoval::new("dev.cowshed.gateway agent", RemovalOutcome::Removed),
            HostArtifactRemoval::new("dev.cowshed.sccache agent", RemovalOutcome::AlreadyAbsent),
            HostArtifactRemoval::new("installed cowshed binary", RemovalOutcome::Removed),
            HostArtifactRemoval::new("installed sccache binary", RemovalOutcome::AlreadyAbsent),
        ],
        ..FakeHost::default()
    };

    let streams = run(&mut host, UNINSTALL, true, false).await;

    assert_eq!(
        streams.stdout,
        "{\"ok\":true,\"result\":{\"fstab\":\"removed\",\"services\":[\
         {\"what\":\"dev.cowshed.storage system LaunchDaemon\",\"outcome\":\"removed\"},\
         {\"what\":\"cowshed.store System.keychain item\",\"outcome\":\"removed\"},\
         {\"what\":\"cowshed.caches System.keychain item\",\"outcome\":\"already-absent\"},\
         {\"what\":\"dev.cowshed.gateway agent\",\"outcome\":\"removed\"},\
         {\"what\":\"dev.cowshed.sccache agent\",\"outcome\":\"already-absent\"},\
         {\"what\":\"installed cowshed binary\",\"outcome\":\"removed\"},\
         {\"what\":\"installed sccache binary\",\"outcome\":\"already-absent\"}\
         ]}}\n"
    );
    assert_eq!(streams.stderr, "");
}

/// A teardown that found nothing installed still reports the empty list rather than omitting it,
/// so a consumer never has to distinguish "no services" from "field missing".
#[tokio::test]
async fn uninstall_json_reports_an_empty_service_list_when_nothing_was_installed() {
    let mut host = FakeHost {
        uninstall_report: uninstall_report(UninstallFstabOutcome::AlreadyClean),
        ..FakeHost::default()
    };

    let streams = run(&mut host, UNINSTALL, true, false).await;

    assert_eq!(
        streams.stdout,
        "{\"ok\":true,\"result\":{\"fstab\":\"alreadyClean\",\"services\":[]}}\n"
    );
}

/// The stderr rendering and the JSON `outcome` token are deliberately different spellings of the
/// same typed value: prose reads "already absent", the wire token is hyphenated like core's action
/// vocabulary. Both come from one match, so neither can drift alone.
#[tokio::test]
async fn removal_prose_and_wire_token_stay_paired() {
    let removals = vec![
        HostArtifactRemoval::new("dev.cowshed.gateway agent", RemovalOutcome::Removed),
        HostArtifactRemoval::new("installed sccache binary", RemovalOutcome::AlreadyAbsent),
    ];

    let mut host = FakeHost {
        uninstall_report: uninstall_report(UninstallFstabOutcome::AlreadyClean),
        removals: removals.clone(),
        ..FakeHost::default()
    };
    let plain = run(&mut host, UNINSTALL, false, false).await;
    assert!(
        plain
            .stderr
            .contains("cowshed: dev.cowshed.gateway agent: removed\n")
    );
    assert!(
        plain
            .stderr
            .contains("cowshed: installed sccache binary: already absent\n")
    );

    let mut host = FakeHost {
        uninstall_report: uninstall_report(UninstallFstabOutcome::AlreadyClean),
        removals,
        ..FakeHost::default()
    };
    let json = run(&mut host, UNINSTALL, true, false).await;
    assert!(json.stdout.contains("\"outcome\":\"removed\""));
    assert!(json.stdout.contains("\"outcome\":\"already-absent\""));
}

/// The verb's whole promise is that a stranded host can type it from anywhere: no project is
/// discovered, and naming one is a usage error rather than a silently ignored flag.
#[test]
fn setup_never_discovers_a_project_from_any_directory() {
    assert_eq!(
        parse_args(["setup"]).unwrap().command.project_discovery(),
        ProjectDiscovery::NotUsed
    );
    assert_eq!(
        parse_args(["setup", "--uninstall", "--force"])
            .unwrap()
            .command
            .project_discovery(),
        ProjectDiscovery::NotUsed
    );

    // Run from a directory that is inside no repository at all: the parse is unchanged, because
    // nothing about this verb consults the working directory.
    let outside = std::env::temp_dir();
    let restore = std::env::current_dir().expect("a current directory");
    std::env::set_current_dir(&outside).expect("chdir outside any repository");
    let parsed = parse_args(["setup"]);
    std::env::set_current_dir(restore).expect("restore the current directory");
    assert_eq!(parsed.unwrap().command, Command::Setup(REPAIR));

    let error = parse_args(["setup", "--project", "/repo"]).unwrap_err();
    assert_eq!(error.message, "--project is not valid for setup");
}

/// Guidance can never name a verb the parser does not have (06_cli.md rule 4), and `setup` is now
/// the verb storage guidance names.
#[test]
fn setup_is_in_the_command_map_between_adopt_and_new() {
    let map = help::command_map();
    let adopt = map.find("\n  adopt").expect("adopt is listed");
    let setup = map.find("\n  setup").expect("setup is listed");
    let new = map.find("\n  new").expect("new is listed");
    assert!(
        adopt < setup && setup < new,
        "setup sits after adopt:\n{map}"
    );

    let spec = help::command_named("setup").expect("setup has a help page");
    // The map prints the spec's own summary rather than a second copy of it, so this asserts the
    // coupling instead of duplicating the sentence and having to be edited alongside it.
    assert!(
        map.contains(spec.summary),
        "map omits the setup summary:\n{map}"
    );
    assert!(spec.hint().contains("--uninstall"));
    assert!(spec.hint().contains("--force"));
    assert!(spec.hint().contains("--mount-root"));
    let page = spec.page();
    assert!(page.contains("--uninstall"));
    assert!(page.contains("--force"));
    assert!(page.contains("--mount-root"));

    // `provision` is cowshed's internal word for minting a volume. Parent's rule is that it never
    // reaches a person, and a help page is the most person-facing surface there is.
    assert!(
        !page.contains("provision"),
        "setup's help page must not say `provision`:\n{page}"
    );
}

/// `--purge` is `gateway stop`'s alone, and it is in the gateway's help page because the parser
/// accepts it.
#[test]
fn gateway_stop_purge_is_parsed_and_documented() {
    assert_eq!(
        parse_args(["gateway", "stop", "--purge"]).unwrap().command,
        Command::Gateway(GatewayCommand::Stop { purge: true })
    );
    assert_eq!(
        parse_args(["gateway", "stop"]).unwrap().command,
        Command::Gateway(GatewayCommand::Stop { purge: false })
    );
    assert!(parse_args(["gateway", "start", "--purge"]).is_err());

    let spec = help::command_named("gateway").expect("gateway has a help page");
    assert!(spec.page().contains("--purge"));
    assert!(help::command_map().contains("[--purge]"));
}

#[tokio::test]
async fn setup_mount_root_prints_the_configured_path() {
    let mut host = FakeHost::default();
    let args = SetupArgs {
        uninstall: false,
        force: false,
        mount_root: Some(PathBuf::from("/Users/dev/.cowshed/mnt")),
    };
    let streams = run(&mut host, args, false, false).await;
    assert_eq!(streams.exit, 0);
    assert_eq!(streams.stdout, "/Users/dev/.cowshed/mnt\n");
    assert!(
        streams
            .stderr
            .contains("workspace mount root is /Users/dev/.cowshed/mnt")
    );
    assert_eq!(
        host.events,
        ["configure-mount-root:/Users/dev/.cowshed/mnt"]
    );
}

#[tokio::test]
async fn setup_mount_root_json_is_empty_success() {
    let mut host = FakeHost::default();
    let args = SetupArgs {
        uninstall: false,
        force: false,
        mount_root: Some(PathBuf::from("/Users/dev/.cowshed/mnt")),
    };
    let streams = run(&mut host, args, true, false).await;
    assert_eq!(streams.exit, 0);
    assert_eq!(streams.stdout, "{\"ok\":true,\"result\":{}}\n");
}

#[tokio::test]
async fn setup_mount_root_refuses_while_workspaces_are_attached() {
    let mut host = FakeHost {
        mount_root_error: Some(CowshedError::conflict(
            "workspace mount root cannot change while attached: acme/widget/swift, zeta/widget/raven",
            "detach every attached workspace, then cowshed setup --mount-root <dir>",
        )),
        ..FakeHost::default()
    };
    let args = SetupArgs {
        uninstall: false,
        force: false,
        mount_root: Some(PathBuf::from("/Users/dev/.cowshed/mnt")),
    };
    let error = refusal(&mut host, args).await;
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(error.message.contains("acme/widget/swift"));
    assert!(error.message.contains("zeta/widget/raven"));
    assert!(error.hint.contains("detach"));
    assert_eq!(
        host.events,
        ["configure-mount-root:/Users/dev/.cowshed/mnt"]
    );
}

/// Mains are always-mounted (02_workspaces.md), so a host with one missing is not one setup may
/// call ready — including the branch a healthy host actually reaches, where the volumes are fine
/// and the plan did nothing. "Everything already set up" over a checkout the user cannot see is
/// the flattest lie this verb could tell.
#[tokio::test]
async fn an_unmounted_main_downgrades_both_healthy_status_lines() {
    let mut host = FakeHost {
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::MountedValid,
                "already-current",
            )],
            fstab: FstabOutcome::AlreadyCurrent,
            authorized: false,
            action_outcomes: Vec::new(),
        },
        census: occupied_census(),
        mains: MainMounts::Checked(detached_main()),
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    // Exit stays 0: setup reports what it found; gateway start remounts mains.
    assert_eq!(streams.exit, 0);
    assert_eq!(
        streams.stderr,
        "cowshed: cowshed.store (store): mounted at its canonical path -> already-current\n\
         cowshed: /etc/fstab already pins the boot mounts\n\
         cowshed: /Users/dev/Library/Application Support/Mozilla.sccache/config already sends a store-less sccache client to /private/cowshed/caches/sccache\n\
         cowshed: everything already set up, but 1 main workspace is not mounted: acme/api\n\
         next: cowshed gateway start\n"
    );

    // The same observation qualifies the repaired-host sentence, and keeps its authorization clause.
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::PinFstab {
                uuid: String::from("1D6F0E1A-0000-4000-8000-00000000AAAA"),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            true,
        ),
        report: HostSetupReport {
            volumes: Vec::new(),
            fstab: FstabOutcome::Pinned,
            authorized: true,
            action_outcomes: Vec::new(),
        },
        census: occupied_census(),
        mains: MainMounts::Checked(detached_main()),
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    assert!(streams.stderr.contains(
        "cowshed: host storage is set up (one administrator authorization was used), but 1 main workspace is not mounted: acme/api\n"
    ));
    assert!(streams.stderr.contains("next: cowshed gateway start\n"));
}

/// "Nobody could check" is its own answer and never renders as "every main is mounted".
#[tokio::test]
async fn mains_that_could_not_be_checked_are_said_so_without_a_remedy() {
    let mut host = FakeHost {
        report: HostSetupReport {
            volumes: Vec::new(),
            fstab: FstabOutcome::AlreadyCurrent,
            authorized: false,
            action_outcomes: Vec::new(),
        },
        census: occupied_census(),
        mains: MainMounts::Unknown {
            reason: String::from("the store is not mounted"),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    assert!(streams.stderr.contains(
        "cowshed: everything already set up; main workspace mounts could not be checked: the store is not mounted\n"
    ));
    // No `gateway start`: an unchecked main is not an observed one, and guessing a remedy for a
    // defect nobody confirmed would send the reader after the wrong problem.
    assert!(!streams.stderr.contains("next: cowshed gateway start"));
}

/// A run that stopped partway keeps its own headline: the failure is the remedy, and a main-mount
/// observation on top of it would aim the reader at the wrong problem. The host is never asked.
#[tokio::test]
async fn a_failed_run_never_observes_or_mentions_main_mounts() {
    let mut host = FakeHost {
        report: HostSetupReport {
            volumes: Vec::new(),
            fstab: FstabOutcome::Skipped(String::from("store volume is not mounted")),
            authorized: false,
            action_outcomes: vec![HostActionOutcome {
                action: HostAction::PinFstab {
                    uuid: String::from("1D6F0E1A-0000-4000-8000-00000000AAAA"),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                },
                outcome: HostActionResult::Failed {
                    error: CowshedError::internal("could not write /etc/fstab"),
                },
            }],
        },
        mains: MainMounts::Checked(detached_main()),
        ..FakeHost::default()
    };

    let (streams, _) = failing_run(&mut host, REPAIR, false).await;

    assert!(
        streams
            .stderr
            .contains("cowshed: host storage is NOT set up:")
    );
    assert!(!streams.stderr.contains("main workspace"));
    assert!(!streams.stderr.contains("next: cowshed gateway start"));
    assert_eq!(
        host.events,
        ["plan", "execute"],
        "a failed run asks nothing further of the host"
    );
}

#[tokio::test]
async fn mount_service_install_is_disclosed_before_authorization() {
    let mut host = FakeHost {
        plan: setup_plan(
            vec![HostAction::InstallMountService {
                label: String::from("dev.cowshed.storage"),
            }],
            true,
        ),
        report: HostSetupReport {
            volumes: Vec::new(),
            fstab: FstabOutcome::AlreadyCurrent,
            authorized: true,
            action_outcomes: Vec::new(),
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(streams.exit, 0);
    assert!(streams.stderr.starts_with(
        "cowshed: setup will request administrator authorization once, for the actions below\n\
         cowshed: no volumes will be created or deleted; existing data is untouched\n\
         cowshed: system LaunchDaemon dev.cowshed.storage will be installed to unlock and mount cowshed volumes before login\n"
    ));
    assert_eq!(
        host.events,
        [
            "plan",
            "execute",
            "unmounted-mains",
            "configure-sccache-client",
            "refresh-services",
            "census"
        ]
    );
}
