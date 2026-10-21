use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_core::api::dto::AdoptOptions;
use cowshed_core::api::operations::OperationRequest;
use cowshed_core::api::server::ConnectionAuthority;
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::storage::WORKSPACE_MARKER_PATH;
use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
use cowshed_os::fork_lock::Run as _;
use cowshed_os::timing::{Step, StepSink};
use serde_json::{Value, json};
use tokio::sync::mpsc;

#[path = "../support/refusing_gateway.rs"]
mod refusing_gateway;

#[path = "../support/scratch_directory.rs"]
mod scratch_directory;

use scratch_directory::ScratchRoot;

fn git(directory: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args([
            "-c",
            "user.name=cowshed",
            "-c",
            "user.email=cowshed@example.invalid",
        ])
        .args(args)
        .output_locked()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn call(
    runtime: &ProjectRuntime,
    repo: &RepoId,
    method: &str,
    params: Value,
    steps: Option<StepSink>,
) -> cowshed_core::Result<Value> {
    let response = runtime
        .router()
        .route(
            ConnectionAuthority::Coordinator {
                repo_id: repo.clone(),
            },
            OperationRequest::decode(method, &params)?,
            None,
            steps,
        )
        .await?;
    Ok(response.into_answer()?.0)
}

/// The doctor steps started and not yet ended, outermost first.
fn open_doctor_steps(reports: &[Step]) -> Vec<String> {
    let mut open: Vec<(u32, String)> = Vec::new();
    for report in reports {
        match report {
            Step::Started {
                step, scope, name, ..
            } if scope == "doctor" => open.push((*step, name.clone())),
            Step::Started { .. } => {}
            Step::Ended { step, .. } => open.retain(|(started, _)| started != step),
        }
    }
    open.into_iter().map(|(_, name)| name).collect()
}

/// A workspace marker that answers nobody until the test writes it stands in for any probe whose
/// answer does not come — a hung filesystem, a daemon that accepted and never replied. The first
/// probe to read it is `workspaces`, the authoritative inventory, which validates every mounted
/// workspace's marker. While it waits, the step its call is in names it; once the answer comes,
/// the step ends, and no doctor step is left open.
#[tokio::test]
async fn a_probe_that_waits_for_an_answer_is_the_doctor_step_its_call_is_in() {
    let scratch = ScratchRoot::new("doctor-progress").expect("scratch directory root");
    let checkout = scratch.path().join("checkout");
    let store = scratch.path().join("store");
    for path in [&checkout, &store] {
        fs::create_dir_all(path).expect("fixture directory");
    }
    git(&checkout, &["init", "-q", "-b", "main"]);
    fs::write(checkout.join("tracked"), b"tracked\n").expect("tracked file");
    fs::write(
        checkout.join(".gitignore"),
        b".envrc\n.envrc-local\n.devenv/\n",
    )
    .expect("workspace hooks ignore");
    git(&checkout, &["add", "tracked", ".gitignore"]);
    git(&checkout, &["commit", "-q", "-m", "initial"]);
    let storage = ValidatedHostStorage::new(
        scratch.path().to_path_buf(),
        Vec::new(),
        CanonicalRoots::at(store),
    );
    let repo = RepoId::parse("fixture/doctor").expect("repository identity");

    let adopting = ProjectRuntime::open_directory_for_adopt_at(
        &checkout,
        Some(repo.clone()),
        storage.clone(),
        refusing_gateway::connect,
    )
    .await
    .expect("open the adopting runtime");
    let adopted = call(
        &adopting,
        &repo,
        "coordinator.adopt",
        json!({
            "repoId": repo,
            "options": AdoptOptions {
                path: Some(checkout.clone()),
                repo_id: Some(repo.clone()),
                capacity: Some("1g".to_owned()),
                quarantine: false,
                re_adopt: false,
            },
        }),
        None,
    )
    .await
    .expect("directory-backed adoption");
    let main_mount = PathBuf::from(
        adopted["workspace"]["info"]["mount"]
            .as_str()
            .expect("main mount"),
    );
    adopting
        .shutdown()
        .await
        .expect("stop the adopting runtime");

    let doctor = ProjectRuntime::open_directory_existing_at(
        &checkout,
        RecoveryScope::Inspect,
        storage,
        refusing_gateway::connect,
    )
    .await
    .expect("doctor's inspecting open");

    // From here on the marker answers only when the test writes it.
    let marker = main_mount.join(WORKSPACE_MARKER_PATH);
    let answer = fs::read(&marker).expect("main's marker");
    fs::remove_file(&marker).expect("remove the marker");
    let made = Command::new("/usr/bin/mkfifo")
        .arg(&marker)
        .status_locked()
        .expect("run mkfifo");
    assert!(made.success(), "mkfifo {}", marker.display());

    let (sender, mut receiver) = mpsc::unbounded_channel();
    let router = doctor.router();
    let doctor_repo = repo.clone();
    let diagnosis = tokio::spawn(async move {
        let response = router
            .route(
                ConnectionAuthority::Coordinator {
                    repo_id: doctor_repo.clone(),
                },
                OperationRequest::decode("coordinator.doctor", &json!({ "repoId": doctor_repo }))?,
                None,
                Some(StepSink::new(sender)),
            )
            .await?;
        Ok::<_, cowshed_core::CowshedError>(response.into_answer()?.0)
    });

    // Opening the write end returns once the probe has opened the read end: it is waiting now.
    let waiting_on = marker.clone();
    let writer =
        tokio::task::spawn_blocking(move || fs::OpenOptions::new().write(true).open(&waiting_on))
            .await
            .expect("open the marker's write end")
            .expect("the probe opened the marker");
    let mut heard = Vec::new();
    while let Ok(report) = receiver.try_recv() {
        heard.push(report);
    }
    assert_eq!(
        open_doctor_steps(&heard).last().map(String::as_str),
        Some("workspaces"),
        "the call waiting on main's marker must be in the doctor step that reads it; heard {heard:?}"
    );

    // Every later read finds the marker a plain file again; then the waiting probe gets its answer.
    let restored = marker.with_extension("restored");
    fs::write(&restored, &answer).expect("stage the marker");
    fs::rename(&restored, &marker).expect("restore the marker");
    {
        use std::io::Write as _;
        let mut writer = writer;
        writer.write_all(&answer).expect("answer the probe");
    }
    diagnosis
        .await
        .expect("doctor task")
        .expect("doctor answers once the marker does");
    while let Ok(report) = receiver.try_recv() {
        heard.push(report);
    }
    assert_eq!(
        open_doctor_steps(&heard),
        Vec::<String>::new(),
        "heard {heard:?}"
    );
    doctor.shutdown().await.expect("stop the doctor runtime");
}
