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
    FstabOutcome, HostSetupPlan, HostSetupReport, HostUninstallPlan, UninstallFstabOutcome,
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
}

impl Default for FakeHost {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            plan: HostSetupPlan {
                actions: Vec::new(),
                requires_authorization: false,
            },
            report: HostSetupReport {
                volumes: Vec::new(),
                fstab: FstabOutcome::AlreadyCurrent,
                authorized: false,
            },
            uninstall_plan: HostUninstallPlan {
                pins_to_remove: Vec::new(),
                requires_authorization: false,
            },
            uninstall_report: UninstallReport {
                fstab: UninstallFstabOutcome::AlreadyClean,
            },
            census: WorkspaceCensus::Counted {
                projects: 0,
                workspaces: 0,
            },
            removals: Vec::new(),
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
        Ok(self.report.clone())
    }

    async fn plan_uninstall(&mut self) -> Result<HostUninstallPlan> {
        self.events.push(String::from("plan-uninstall"));
        Ok(self.uninstall_plan.clone())
    }

    async fn execute_uninstall(&mut self) -> Result<UninstallReport> {
        self.events.push(String::from("execute-uninstall"));
        Ok(self.uninstall_report.clone())
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
         next: cowshed doctor\n"
    );
    assert_eq!(host.events, ["plan", "execute"]);
}

/// 06_cli.md rule 3: the sentence naming the prompt is printed *before* the phase that raises it.
/// The ordering is the assertion — the announcement has to precede `execute`, not accompany it.
#[tokio::test]
async fn an_escalating_run_announces_the_prompt_before_executing() {
    let mut host = FakeHost {
        plan: HostSetupPlan {
            actions: vec![
                String::from("create volume cowshed.store"),
                String::from("pin /private/cowshed/store in /etc/fstab"),
            ],
            requires_authorization: true,
        },
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Absent,
                "provisioned",
            )],
            fstab: FstabOutcome::Pinned,
            authorized: true,
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization to provision/remount cowshed volumes\n\
         cowshed: planned: create volume cowshed.store\n\
         cowshed: planned: pin /private/cowshed/store in /etc/fstab\n\
         cowshed: cowshed.store (store): absent -> provisioned\n\
         cowshed: pinned the boot mounts in /etc/fstab\n\
         cowshed: host storage is set up (one administrator authorization was used)\n\
         next: cowshed doctor\n"
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
    assert_eq!(host.events, ["plan", "execute"]);
}

/// A run with nothing to escalate raises no prompt, so it says nothing about one.
#[tokio::test]
async fn a_run_that_cannot_escalate_never_mentions_authorization() {
    let mut host = FakeHost {
        plan: HostSetupPlan {
            actions: vec![String::from("remount cowshed.caches at /private/cowshed/caches")],
            requires_authorization: false,
        },
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
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert!(!streams.stderr.contains("authorization"));
    assert_eq!(
        streams.stderr,
        "cowshed: planned: remount cowshed.caches at /private/cowshed/caches\n\
         cowshed: cowshed.caches (caches): mis-mounted at /Volumes/cowshed.caches -> remounted\n\
         cowshed: /etc/fstab already pins the boot mounts\n\
         cowshed: host storage is set up\n\
         next: cowshed doctor\n"
    );
}

/// A volume in another container is its own state with its own guidance, never "missing", and the
/// status line refuses to call the host set up while it stands. Re-provisioning it would mean
/// `deleteVolume`, so the one thing this row must never imply is that setup could fix it.
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
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, false).await;

    assert_eq!(
        streams.stderr,
        "cowshed: cowshed.store (store): found outside this host's container (container disk4, device disk4s7) -> reported\n\
         cowshed: data is safe on disk4s7; not provisioned (readable at /Volumes/cowshed.store)\n\
         cowshed: cowshed.caches (caches): found outside this host's container (container disk4, device disk4s8) -> reported\n\
         cowshed: data is safe on disk4s8; not provisioned\n\
         cowshed: /etc/fstab not pinned: no cowshed volume in the home container\n\
         cowshed: host storage is partially set up: 2 volumes live outside this host's container and left untouched\n\
         next: cowshed doctor\n"
    );
    assert!(!streams.stderr.contains("absent"));
    assert!(!streams.stderr.contains("everything already set up"));
}

/// `--json` puts exactly one frozen envelope on stdout and keeps prose off it. The rendered rows
/// are the human view of the same report, so they are not repeated on stderr.
#[tokio::test]
async fn json_emits_one_frozen_envelope_and_no_prose_on_stdout() {
    let mut host = FakeHost {
        plan: HostSetupPlan {
            actions: vec![String::from("create volume cowshed.store")],
            requires_authorization: true,
        },
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
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, true, false).await;

    assert_eq!(
        streams.stdout,
        "{\"ok\":true,\"result\":{\"volumes\":[{\"name\":\"cowshed.store\",\"role\":\"store\",\
         \"stateBefore\":{\"foundElsewhere\":{\"container\":\"disk4\",\"device\":\"disk4s7\",\
         \"mountedAt\":null}},\"action\":\"reported\"}],\"fstab\":\"pinned\",\"authorized\":true}}\n"
    );
    // The announcement is guidance, not an answer: it stays on stderr even in JSON mode, because a
    // dialog is about to appear either way.
    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization to provision/remount cowshed volumes\n\
         cowshed: planned: create volume cowshed.store\n\
         next: cowshed doctor\n"
    );
}

/// `-q` drops guidance and keeps the hint — and keeps the authorization announcement, which is not
/// guidance a caller can opt out of.
#[tokio::test]
async fn quiet_suppresses_rows_but_never_the_prompt_announcement_or_the_hint() {
    let mut host = FakeHost {
        plan: HostSetupPlan {
            actions: vec![String::from("create volume cowshed.store")],
            requires_authorization: true,
        },
        report: HostSetupReport {
            volumes: vec![volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Absent,
                "provisioned",
            )],
            fstab: FstabOutcome::Pinned,
            authorized: true,
        },
        ..FakeHost::default()
    };

    let streams = run(&mut host, REPAIR, false, true).await;

    assert_eq!(
        streams.stderr,
        "cowshed: setup will request administrator authorization to provision/remount cowshed volumes\n\
         next: cowshed doctor\n"
    );
    assert_eq!(streams.stdout, "");
}

/// Teardown removes the machine presence and says, every time, that the data is still there.
#[tokio::test]
async fn uninstall_removes_host_presence_and_names_what_survives() {
    let mut host = FakeHost {
        uninstall_plan: HostUninstallPlan {
            pins_to_remove: vec![String::from("UUID=1111 /private/cowshed/store")],
            requires_authorization: true,
        },
        uninstall_report: UninstallReport {
            fstab: UninstallFstabOutcome::Removed,
        },
        census: WorkspaceCensus::Counted {
            projects: 0,
            workspaces: 0,
        },
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
         cowshed: planned: UUID=1111 /private/cowshed/store\n\
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
        census: WorkspaceCensus::Counted {
            projects: 2,
            workspaces: 5,
        },
        uninstall_report: UninstallReport {
            fstab: UninstallFstabOutcome::Removed,
        },
        ..FakeHost::default()
    };

    let mut host = occupied();
    let error = refusal(&mut host, UNINSTALL).await;
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(
        error.message,
        "5 workspaces still exist on this host's volumes across 2 adopted projects; \
         uninstall removes no volume and no image, so they would be left unmanaged"
    );
    assert_eq!(error.hint, "cowshed setup --uninstall --force");
    assert_eq!(host.events, ["census"], "a refusal touches nothing");

    let mut host = occupied();
    let streams = run(&mut host, FORCED_UNINSTALL, false, false).await;
    assert!(streams.stderr.contains(
        "cowshed: cowshed's host presence is removed; 5 workspaces and their images are still on the volumes, which were not touched\n"
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
    assert_eq!(error.hint, "cowshed setup --uninstall --force");

    let mut host = unknown();
    let streams = run(&mut host, FORCED_UNINSTALL, false, false).await;
    assert!(streams.stderr.contains(
        "cowshed: cowshed's host presence is removed; volume contents were never inspected and no volume was touched\n"
    ));
}

/// Teardown's `--json` carries the core report and nothing else; the service teardown stays on
/// stderr, because the frozen envelope has exactly one named result body.
#[tokio::test]
async fn uninstall_json_carries_the_frozen_uninstall_report() {
    let mut host = FakeHost {
        uninstall_report: UninstallReport {
            fstab: UninstallFstabOutcome::AlreadyClean,
        },
        removals: vec![HostArtifactRemoval::new(
            "installed cowshed binary",
            RemovalOutcome::AlreadyAbsent,
        )],
        ..FakeHost::default()
    };

    let streams = run(&mut host, UNINSTALL, true, false).await;

    assert_eq!(
        streams.stdout,
        "{\"ok\":true,\"result\":{\"fstab\":\"alreadyClean\"}}\n"
    );
    assert_eq!(streams.stderr, "next: cowshed doctor\n");
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
    assert!(map.contains("provision or repair host storage and pin mounts"));

    let spec = help::command_named("setup").expect("setup has a help page");
    assert_eq!(spec.hint(), "cowshed setup [--uninstall] [--force]");
    let page = spec.page();
    assert!(page.contains("--uninstall"));
    assert!(page.contains("--force"));
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
