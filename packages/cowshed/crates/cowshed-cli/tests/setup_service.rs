//! Contract tests for the `setup` verb.
//!
//! Everything asserted here is behaviour a caller can observe — parsed grammar, exact stdout and
//! stderr bytes, the frozen JSON envelope, the exit-shaping of a refusal, and the order in which
//! the host is touched. Nothing asserted here depends on how the arguments are parsed, so a
//! replacement parser is held to the same contract.

use async_trait::async_trait;
use cowshed_cli::args::{Command, GatewayCommand, ProjectDiscovery, SetupArgs, parse_args};
use cowshed_cli::help;
use cowshed_cli::launchd::RemovalOutcome;
use cowshed_cli::output::Output;
use cowshed_cli::setup_service::{
    HostArtifactRemoval, HostSetup, WorkspaceCensus, dispatch as setup_dispatch,
};
use cowshed_core::storage::bootstrap::{
    FstabOutcome, HostAction, HostSetupPlan, HostSetupReport, HostUninstallPlan,
    UninstallFstabOutcome,
    UninstallReport, VolumeOutcome, VolumeRole, VolumeState,
};
use cowshed_core::{CowshedError, ErrorCode, Result};
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
    /// What the escalating phase fails with, so the decline path is provable without a dialog.
    execute_error: Option<CowshedError>,
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
            execute_error: None,
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

    async fn remove_host_services(&mut self) -> Result<Vec<HostArtifactRemoval>> {
        self.events.push(String::from("remove-host-services"));
        Ok(self.removals.clone())
    }
}

const REPAIR: SetupArgs = SetupArgs {
    uninstall: false,
    force: false,
};
const UNINSTALL: SetupArgs = SetupArgs {
    uninstall: true,
    force: false,
};
const FORCED_UNINSTALL: SetupArgs = SetupArgs {
    uninstall: true,
    force: true,
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
         cowshed: everything already set up\n\
         next: cowshed doctor\n\
         next: cowshed adopt\n"
    );
    assert_eq!(host.events, ["plan", "execute", "census"]);
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
         cowshed: host storage is set up (one administrator authorization was used)\n\
         next: cowshed doctor\n\
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
    assert_eq!(host.events, ["plan", "execute", "census"]);
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
                    mount_at: PathBuf::from("/Users/danny/.cowshed"),
                },
                HostAction::PinFstab {
                    uuid: String::from("1D6F0E1A-0000-4000-8000-00000000AAAA"),
                    mount_at: PathBuf::from("/Users/danny/.cowshed"),
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
         cowshed: cowshed.store exists (UUID 1D6F0E1A-0000-4000-8000-00000000AAAA, 1.0 TB) and will be mounted at /Users/danny/.cowshed\n\
         cowshed: /etc/fstab will pin UUID 1D6F0E1A-0000-4000-8000-00000000AAAA at /Users/danny/.cowshed so it mounts at every boot\n\
         cowshed: cowshed.store (store): present but not mounted -> mounted\n\
         cowshed: pinned the boot mounts in /etc/fstab\n\
         cowshed: host storage is set up (one administrator authorization was used)\n\
         next: cowshed doctor\n\
         next: cowshed adopt\n"
    );
    assert!(!streams.stderr.contains("provision"));
}

/// A plan that creates a volume cannot make the safety promise, and must not.
#[tokio::test]
async fn a_plan_that_creates_a_volume_makes_no_safety_promise() {
    let mut host = FakeHost::default();
    host.plan = setup_plan(
        vec![HostAction::CreateVolume {
            name: String::from("cowshed.store"),
            container: String::from("disk3"),
            mount_at: PathBuf::from("/private/cowshed/store"),
        }],
        true,
    );

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(!streams.stderr.contains("no volumes will be created or deleted"));
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

    assert!(!streams.stderr.contains("no volumes will be created or deleted"));
    assert!(streams.stderr.contains("cowshed: everything already set up\n"));
}

/// Reclaimable stubs are named, never counted: "3 files will be deleted" is not something a
/// person can agree to (01_storage.md).
#[tokio::test]
async fn reclaimable_stubs_are_enumerated_by_name() {
    let mut host = FakeHost::default();
    host.plan = setup_plan(
        vec![HostAction::ReclaimStubs {
            paths: vec![
                PathBuf::from("/private/cowshed/store/.envrc"),
                PathBuf::from("/private/cowshed/store/telemetry"),
            ],
        }],
        false,
    );

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
        let mut host = FakeHost::default();
        host.plan = setup_plan(
            vec![HostAction::MountExisting {
                name: String::from("cowshed.store"),
                uuid: String::from("U"),
                size_bytes: bytes,
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            false,
        );

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
         cowshed: host storage is set up\n\
         next: cowshed doctor\n\
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
         cowshed: host storage is partially set up: 2 volumes live outside this host's container and left untouched\n\
         next: cowshed doctor\n\
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
         next: cowshed doctor\n\
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
         next: cowshed doctor\n\
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
         cowshed: cowshed's host presence is removed; no workspaces existed and no volume was touched\n\
         next: cowshed doctor\n"
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

/// Teardown's `--json` carries the whole outcome, not just core's half.
///
/// Core returns `services` empty — the fstab pins are all it owns — so this asserts that the
/// adapter fills it. Order matters and is frozen: both agents, then both binaries, because the
/// gateway agent is `KeepAlive` and deleting its binary under a loaded agent would leave launchd
/// respawning a vanished path. The `what` and `outcome` vocabularies are frozen here because this
/// is the only place they are produced.
#[tokio::test]
async fn uninstall_json_reports_the_services_the_adapter_removed() {
    let mut host = FakeHost {
        uninstall_report: uninstall_report(UninstallFstabOutcome::Removed),
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
         {\"what\":\"dev.cowshed.gateway agent\",\"outcome\":\"removed\"},\
         {\"what\":\"dev.cowshed.sccache agent\",\"outcome\":\"already-absent\"},\
         {\"what\":\"installed cowshed binary\",\"outcome\":\"removed\"},\
         {\"what\":\"installed sccache binary\",\"outcome\":\"already-absent\"}\
         ]}}\n"
    );
    assert_eq!(streams.stderr, "next: cowshed doctor\n");
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
    assert!(plain
        .stderr
        .contains("cowshed: dev.cowshed.gateway agent: removed\n"));
    assert!(plain
        .stderr
        .contains("cowshed: installed sccache binary: already absent\n"));

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
    assert!(adopt < setup && setup < new, "setup sits after adopt:\n{map}");

    let spec = help::command_named("setup").expect("setup has a help page");
    // The map prints the spec's own summary rather than a second copy of it, so this asserts the
    // coupling instead of duplicating the sentence and having to be edited alongside it.
    assert!(map.contains(spec.summary), "map omits the setup summary:\n{map}");
    assert_eq!(spec.hint(), "cowshed setup [--uninstall] [--force]");
    let page = spec.page();
    assert!(page.contains("--uninstall"));
    assert!(page.contains("--force"));

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
