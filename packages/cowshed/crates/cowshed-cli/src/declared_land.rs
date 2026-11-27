//! A declared land (02_workspaces.md "A declared land"): `cowshed land <ws>` on a repository whose
//! main `.cowshed.toml` declares `[land]` is the whole route from a committed shed to a pushed
//! main. This is its shell: it takes the turns, runs the declared commands and asks the service
//! for the rebase, the fast-forward and the retire. Every decision over what it observes is
//! [`cowshed_core::declared_land`]'s.

use std::ffi::OsString;
use std::io::Write;
use std::os::fd::AsFd as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use cowshed_core::api::dto::{
    Adoption, AdoptionSkip, ExpectedRefHead, GitOid, LandBuildVolume, LandOptions, LandReport,
    LandSeed, LandTurnName, LandedReport, RebaseOptions, RebaseResume,
};
use cowshed_core::declared_land::{
    self as declared, AfterFastForward, AfterRequest, LandDeclaration, LandEvent, LandRecord,
};
use cowshed_core::land_turn::{Turn, TurnAsk};
use cowshed_core::runtime::ref_turn::{RefTurn, RefWriter};
use cowshed_core::{CowshedError, ErrorCode, FenceRefusal, Result};
use cowshed_os::fork_lock::Run as _;

use crate::output::Output;
use crate::runtime::{CliService, output_error};

/// Internal verb invoked on the installed cowshed binary when a declared land's turn resumed
/// across a gateway build change mid-gate (02_workspaces.md "A declared land" step 5,
/// 05_gateway.md "A land step waits for an install in progress").
pub const STEP5_VERB: &str = "__land-step5";
const STEP5_PROOF_VERSION: &str = "1";

/// What a declared land was asked for beyond its workspace.
pub(crate) struct DeclaredLand<'a> {
    pub workspace: &'a str,
    pub retire: bool,
    pub push: bool,
    /// Run the fences and the declared gate under the land turn and stop: nothing moves.
    pub gate_only: bool,
}

/// Refuse what a declared land pins or runs itself (02_workspaces.md "A declared land"): the
/// primitive's caller-side fences and checks.
pub(crate) fn refuse_primitive_options(args: &crate::args::LandArgs) -> Result<()> {
    let refused: Vec<&str> = [
        (!args.checks.is_empty(), "--check"),
        (args.target.is_some(), "--target"),
        (
            args.expected_workspace_incarnation.is_some(),
            "--expected-workspace-incarnation",
        ),
        (
            args.expected_source_head.is_some(),
            "--expected-source-head",
        ),
        (
            args.expected_target_head.is_some(),
            "--expected-target-head",
        ),
        (args.ref_lock.is_some(), "--ref-lock"),
        (args.ref_turn.is_some(), "--ref-turn"),
        (args.push_only, "--push-only"),
        (!args.mode.is_gated(), "--ungated"),
    ]
    .into_iter()
    .filter_map(|(given, option)| given.then_some(option))
    .collect();
    if refused.is_empty() {
        return Ok(());
    }
    Err(CowshedError::usage(
        format!(
            "this repository declares its land route in main's .cowshed.toml [land], which pins its own heads, gate and ref turn; {} cannot be given to it",
            refused.join(", ")
        ),
        format!(
            "cowshed land {} [--no-push] [--no-retire] [--gate-only]",
            args.workspace
        ),
    ))
}

/// Run the declared route for `land` in the repository whose main checkout is `main`.
pub(crate) async fn land<S, W, E>(
    service: &mut S,
    main: &Path,
    declaration: &LandDeclaration,
    land: DeclaredLand<'_>,
    step: Option<&crate::install_basis::StepStart>,
    output: &mut Output<W, E>,
) -> Result<LandReport>
where
    S: CliService,
    W: Write + Send,
    E: Write + Send,
{
    let fallback_step;
    let step = match step {
        Some(step) => step,
        None => {
            fallback_step = crate::install_basis::StepStart::at(&service.support_directory()?);
            &fallback_step
        }
    };
    let root = service.land_turn_root()?;
    let turn_directory = root.join(declaration.turn().as_str());
    let environment = pure_environment(|name| std::env::var_os(name))?;
    // Refuse before the turn what the land would refuse after its gate.
    let preflight = service
        .land(
            land.workspace,
            None,
            LandOptions {
                preflight: true,
                ..LandOptions::default()
            },
        )
        .await?;
    let LandReport::Preflighted(preflight) = preflight else {
        return Err(CowshedError::internal(format!(
            "a preflight of {} answered {preflight:?}, not a preflight report",
            land.workspace
        )));
    };
    let mut turn = crate::land_turn::take_turn(
        &service.control_socket()?,
        TurnAsk {
            repo: declaration.turn().clone(),
            workspace: land.workspace.to_owned(),
            command: format!("cowshed land {}", land.workspace),
            class: None,
            resumable: true,
        },
        None,
        output,
    )
    .await?;
    // An install restarts the gateway only while no queue holds a ticket (05_gateway.md "The read
    // fences the turns"), so the one stretch of this land it can fall in is before its turn: from
    // the step's start to the ask. A daemon that became another build there refuses this process
    // at its first supervisor, after the fast-forward. So the land asks the daemon's build once,
    // holding its turn, and ends before anything moves when it is another; `run` then goes on as
    // the installed cowshed (05_gateway.md "A land step waits for an install in progress").
    if let Err(refused) = service.require_daemon_of_this_build().await {
        let _ = turn.end(format!("refused: {}", refused.message));
        return Err(refused);
    }
    if land.gate_only {
        // The gate alone (02_workspaces.md "A declared land", "The gate alone"): the fences and
        // the turn are the land's, so it never runs beside a land's gate, and what it answers is
        // what a land of this head would be told.
        let gated = gate_alone(
            main,
            declaration,
            &land,
            &preflight,
            &environment,
            &mut turn,
            output,
        )
        .await;
        return match gated {
            Ok(()) => {
                let _ = turn.end(format!("gated {}", preflight.source_head.as_str()));
                say(
                    output,
                    &format!(
                        "the gate passed for {} of {}; nothing was rebased, landed or retired, and the land turn {} is free",
                        preflight.source_head.as_str(),
                        land.workspace,
                        declaration.turn().as_str()
                    ),
                )?;
                Ok(LandReport::Preflighted(preflight))
            }
            Err(error) => {
                let _ = turn.end(format!("refused: {}", error.message));
                Err(error)
            }
        };
    }
    let routed = route(
        service,
        main,
        declaration,
        &land,
        &preflight.target_branch,
        &environment,
        &mut turn,
        output,
    )
    .await;
    let routed = match routed {
        Ok(routed) => {
            let _ = turn.end(match &routed.retire {
                Ok(_) => format!("landed {}", routed.head.as_str()),
                Err(refused) => format!(
                    "landed {}; its retire was refused: {}",
                    routed.head.as_str(),
                    refused.message
                ),
            });
            routed
        }
        Err(error) => {
            let _ = turn.end(format!("refused: {}", error.message));
            return Err(error);
        }
    };
    say(
        output,
        &format!(
            "landed {} on {}; the land turn {} is free",
            routed.head.as_str(),
            routed.branch,
            declaration.turn().as_str()
        ),
    )?;
    note(
        output,
        &turn_directory,
        LandRecord::now(
            LandEvent::Landed,
            routed.head.clone(),
            Some(routed.before.clone()),
            None,
        ),
    )?;
    if !land.push {
        note(
            output,
            &turn_directory,
            LandRecord::now(
                LandEvent::Unpushed,
                routed.head.clone(),
                None,
                Some("cowshed land --no-push".to_owned()),
            ),
        )?;
    }
    // Main has moved, whatever the retire answered: the push is keyed on that alone.
    let push = declaration
        .push()
        .filter(|_| land.push && routed.before != routed.head)
        .cloned();
    let after: Vec<Vec<String>> = declaration
        .after()
        .iter()
        .map(|command| command.argv(&routed.before, &routed.head))
        .collect();
    if push.is_some() || !after.is_empty() {
        let pushing = push.is_some();
        let request = AfterRequest {
            main: main.to_owned(),
            turn: turn_directory.clone(),
            branch: routed.branch.clone(),
            before: routed.before.clone(),
            head: routed.head.clone(),
            push,
            after,
            environment: environment
                .iter()
                .map(|(name, value)| {
                    (
                        name.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
                .collect(),
        };
        let started = start_after(service, &request, step).await;
        record_after_outcome(output, &turn_directory, &routed.head, pushing, started)?;
    }
    let landed = routed.retire?;
    if landed.retired {
        service.reconcile_gateway().await?;
    }
    Ok(LandReport::Landed(landed))
}

/// What the route did to main: the head it landed, onto which, and the primitive's answer for
/// the retire that followed the fast-forward (its refusal, when main moved but the workspace was
/// kept).
struct Routed {
    head: GitOid,
    branch: String,
    before: GitOid,
    retire: Result<LandedReport>,
}

/// The declared gate on `land`'s workspace as it stands, under the land turn: no sync with the
/// push remote, no rebase, no fast-forward and no retire.
async fn gate_alone<W, E>(
    main: &Path,
    declaration: &LandDeclaration,
    land: &DeclaredLand<'_>,
    preflight: &cowshed_core::api::dto::LandPreflight,
    environment: &[(OsString, OsString)],
    turn: &mut Turn,
    output: &mut Output<W, E>,
) -> Result<()>
where
    W: Write + Send,
    E: Write + Send,
{
    say(
        output,
        &format!(
            "gating {} as it stands with {}; nothing is rebased or landed",
            preflight.source_head.as_str(),
            declaration.gate().program()
        ),
    )?;
    if let Some(lost) = lost_now(turn).await {
        return Err(turn_lost(declaration.turn(), "before the gate", &lost));
    }
    gate(main, declaration, land.workspace, environment, turn).await
}

/// Submit the after-land work for `request` to main's workspace supervisor as a background job at
/// the host boundary (02_workspaces.md "A declared land" step 6), and answer its job id without
/// waiting; why not, when it was not admitted.
async fn start_after<S: CliService>(
    service: &mut S,
    request: &AfterRequest,
    step: &crate::install_basis::StepStart,
) -> std::result::Result<AdmittedAfter, String> {
    let program = service
        .background_program()
        .map_err(|error| error.message)?
        .ok_or_else(|| "this store names no cowshed executable for background work".to_owned())?;
    start_after_with(&program, request, step, |command| async move {
        service.require_daemon_of_this_build().await?;
        service
            .exec(
                command,
                crate::runtime::ExecPresentation::Control,
                &mut std::io::sink(),
                &mut std::io::sink(),
            )
            .await
            .map(|result| result.info.job_id.get())
    })
    .await
}
/// Outcome of admitting the after-land job.
#[derive(Debug, Eq, PartialEq)]
struct AdmittedAfter {
    job: u64,
    /// Present when the installed copy printed a valid job id (proving admission) and then exited
    /// non-zero (for example, a teardown failure after admission).
    post_admission_exit: Option<PostAdmissionExit>,
}

/// A non-zero exit from the installed cowshed copy after it had already printed the admitted job
/// id on stdout.
#[derive(Debug, Eq, PartialEq)]
struct PostAdmissionExit {
    program: String,
    status: String,
    stderr: String,
}

fn record_after_outcome<W: Write + Send, E: Write + Send>(
    output: &mut Output<W, E>,
    turn_directory: &Path,
    head: &GitOid,
    pushing: bool,
    started: std::result::Result<AdmittedAfter, String>,
) -> Result<()> {
    match started {
        Ok(AdmittedAfter {
            job,
            post_admission_exit,
        }) => {
            say(
                output,
                &format!(
                    "the push and after-land commands run as job {job} of main, never awaited; their outcomes go to {}",
                    turn_directory.join(declared::LANDS_LOG).display(),
                ),
            )?;
            if let Some(exited) = post_admission_exit {
                let message = if exited.stderr.is_empty() {
                    format!(
                        "the installed cowshed ({}) {} after admitting job {job}",
                        exited.program, exited.status
                    )
                } else {
                    format!(
                        "the installed cowshed ({}) {} after admitting job {job}: {}",
                        exited.program, exited.status, exited.stderr
                    )
                };
                output.warning(&message).map_err(output_error)?;
            }
            Ok(())
        }
        Err(reason) => {
            let detail = format!(
                "the after-land job was not admitted: {reason}; neither the push nor any after-land command ran"
            );
            note(
                output,
                turn_directory,
                LandRecord::now(
                    if pushing {
                        LandEvent::PushFailed
                    } else {
                        LandEvent::AfterExited
                    },
                    head.clone(),
                    None,
                    Some(detail.clone()),
                ),
            )?;
            output
                .warning(&format!("landed {}, but {detail}", head.as_str()))
                .map_err(output_error)
        }
    }
}

async fn start_after_with<F, Fut>(
    program: &Path,
    request: &AfterRequest,
    step: &crate::install_basis::StepStart,
    submit: F,
) -> std::result::Result<AdmittedAfter, String>
where
    F: FnOnce(crate::runtime::ExecCommand) -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let argv = crate::land_after::argv(program, request).map_err(|error| error.message)?;
    let rerun = argv
        .iter()
        .map(|word| shell_word(&word.to_string_lossy()))
        .collect::<Vec<_>>()
        .join(" ");
    let command = crate::runtime::ExecCommand {
        workspace: cowshed_core::git::MAIN_REMOTE.to_owned(),
        request: cowshed_core::api::ExecRequest {
            command: cowshed_core::api::ExecCommand::Argv(
                argv.iter()
                    .cloned()
                    .map(cowshed_core::api::CommandArg::from)
                    .collect(),
            ),
            cwd: None,
            mode: cowshed_core::api::RunSandboxMode::ReadWrite,
            env: request.environment.iter().cloned().collect(),
            trace: None,
            stdin: cowshed_core::api::StdinSource::Empty,
            stdout_copy: None,
            stderr_copy: None,
            admission_key: None,
            class: cowshed_core::api::ExecClass::Background,
            lander: None,
            want: None,
            hold: false,
            scope: cowshed_core::api::ExecScope::HostGate,
            record: cowshed_core::api::JobRecord::Cowshed,
        },
        session: None,
        background: true,
        timeout: None,
    };
    // The request is written. If the daemon became another build after an install replaced the
    // stable name since this land began, hand the submission off once to the installed copy;
    // any other refusal, an unreplaced stable name, or a failed hand-off records push-failed with
    // the by-hand command.
    match submit(command).await {
        Ok(job) => Ok(AdmittedAfter {
            job,
            post_admission_exit: None,
        }),
        Err(error) if error.other_build_source().is_some() && step.replaced() => {
            submit_after_as_installed(step.support(), request, &argv, &rerun).await
        }
        Err(error) => Err(not_admitted(&error, &rerun)),
    }
}

/// Submit the already-written after-land request once through the installed cowshed's `exec`
/// verb (`--host --background`, carrying `request.environment` verbatim). Never loops: a failure
/// here returns the reason plus the by-hand rerun command.
async fn submit_after_as_installed(
    support: &Path,
    request: &AfterRequest,
    argv: &[OsString],
    rerun: &str,
) -> std::result::Result<AdmittedAfter, String> {
    let mut command = crate::install_basis::installed_command(support)
        .map_err(|error| not_admitted(&error, rerun))?;
    let program = command.get_program().to_string_lossy().into_owned();
    command
        .arg("--project")
        .arg(&request.main)
        .arg("--quiet")
        .args([
            "exec",
            cowshed_core::git::MAIN_REMOTE,
            "--host",
            "--background",
            "--",
        ])
        .args(argv)
        .env_clear()
        .envs(
            request
                .environment
                .iter()
                .map(|(name, value)| (name, value)),
        );
    let (status, stdout, stderr) = crate::doc_commit::run_split(command, None, None)
        .await
        .map_err(|error| not_admitted(&error, rerun))?;
    let output = String::from_utf8_lossy(&stdout);
    if let Some(job) = parse_admitted_job_id(&output) {
        let post_admission_exit = (!status.success()).then(|| PostAdmissionExit {
            program,
            status: describe(status),
            stderr: String::from_utf8_lossy(&stderr).trim().to_owned(),
        });
        return Ok(AdmittedAfter {
            job,
            post_admission_exit,
        });
    }
    if !status.success() {
        let detail = String::from_utf8_lossy(&stderr).trim().to_owned();
        let reason = if detail.is_empty() {
            format!(
                "the installed cowshed ({program}) failed to submit the after-land job ({})",
                describe(status)
            )
        } else {
            format!(
                "the installed cowshed ({program}) failed to submit the after-land job ({}): {detail}",
                describe(status)
            )
        };
        return Err(format!("{reason}; run it by hand: {rerun}"));
    }
    Err(format!(
        "the installed cowshed ({program}) did not report a job id (stdout: {}); run it by hand: {rerun}",
        output.trim()
    ))
}

/// Parse stdout from `cowshed exec --background` as one bare unsigned job id: non-empty ASCII
/// digits alone (after trimming surrounding whitespace), rejecting signs, extra lines, or prose.
fn parse_admitted_job_id(stdout: &str) -> Option<u64> {
    let trimmed = stdout.trim();
    (!trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| trimmed.parse::<u64>().ok())
        .flatten()
}

/// Why the after-land job was not admitted, and the command that runs it by hand: `rerun`, the
/// job's own argv, whose program is the installed cowshed's stable name.
fn not_admitted(error: &CowshedError, rerun: &str) -> String {
    format!("{}; run it by hand: {rerun}", error.message)
}

/// `word` as one shell word.
pub(crate) fn shell_word(word: &str) -> String {
    if !word.is_empty()
        && word
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_./:=@%+-".contains(&byte))
    {
        word.to_owned()
    } else {
        format!("'{}'", word.replace('\'', r"'\''"))
    }
}

/// Steps 3 to 5 under the land turn: rebase, gate, fast-forward and the primitive's retire; back
/// to the rebase while main moved under the gate. `branch` is the preflight's target branch, the
/// one a land whose retire was refused moved.
#[allow(clippy::too_many_arguments)]
async fn route<S, W, E>(
    service: &mut S,
    main: &Path,
    declaration: &LandDeclaration,
    land: &DeclaredLand<'_>,
    branch: &str,
    environment: &[(OsString, OsString)],
    turn: &mut Turn,
    output: &mut Output<W, E>,
) -> Result<Routed>
where
    S: CliService,
    W: Write + Send,
    E: Write + Send,
{
    let workspace = land.workspace;
    let mut regated = 0_u32;
    loop {
        holding(
            turn,
            declaration.turn(),
            "the sync with the push remote",
            // Boxed: its remote reads inline otherwise into every dispatch future, and a debug test
            // thread's stack overflowed on the larger one.
            Box::pin(sync_with_push_remote(
                &*service,
                main,
                declaration,
                workspace,
                branch,
                output,
            )),
        )
        .await?;
        let (base, head) = holding(
            turn,
            declaration.turn(),
            "the rebase",
            rebase(service, main, workspace, output),
        )
        .await?;
        say(
            output,
            &format!(
                "gating {} (rebased onto {}) with {}",
                head.as_str(),
                base.as_str(),
                declaration.gate().program()
            ),
        )?;
        if let Some(lost) = lost_now(turn).await {
            return Err(turn_lost(declaration.turn(), "before the gate", &lost));
        }
        gate(main, declaration, workspace, environment, turn).await?;
        if let Err(lost) = turn.wait_active().await {
            return Err(turn_lost(
                declaration.turn(),
                "before the fast-forward",
                &lost,
            ));
        }
        if let Some(lost) = lost_now(turn).await {
            return Err(turn_lost(
                declaration.turn(),
                "before the fast-forward",
                &lost,
            ));
        }
        if turn.resumed_across_restart() {
            match service.require_daemon_of_this_build().await {
                Ok(()) => {}
                Err(refused) if refused.other_build_source().is_some() => {
                    return handoff_step_5(
                        service,
                        main,
                        declaration,
                        land,
                        branch,
                        base,
                        head,
                        turn,
                    )
                    .await;
                }
                Err(error) => return Err(error),
            }
        }
        let fast_forward = service
            .land(
                workspace,
                None,
                LandOptions {
                    retire: land.retire,
                    expected_source_head: Some(head.clone()),
                    expected_target_head: Some(ExpectedRefHead::Oid(base.clone())),
                    ref_turn: Some(declaration.ref_turn().clone()),
                    ..LandOptions::default()
                },
            )
            .await;
        let refused = match fast_forward {
            Ok(LandReport::Landed(landed)) => {
                return Ok(Routed {
                    head: landed.landed_head.clone(),
                    branch: landed.target_branch.clone(),
                    before: base,
                    retire: Ok(landed),
                });
            }
            Ok(other) => {
                return Err(CowshedError::internal(format!(
                    "a gated land of {workspace} into main answered {other:?}, not a landed report"
                )));
            }
            Err(refused) => refused,
        };
        let delivered =
            refused.fence_source().is_none() && head != base && main_holds(main, &head)?;
        match declared::after_refused_fast_forward(refused.fence_source(), delivered) {
            AfterFastForward::Landed => {
                return Ok(Routed {
                    head,
                    branch: branch.to_owned(),
                    before: base,
                    retire: Err(refused),
                });
            }
            AfterFastForward::Regate { observed } => {
                regated += 1;
                say(
                    output,
                    &format!(
                        "main moved from {} to {} while the gate ran; rebasing onto it and gating again (re-gate {regated})",
                        base.as_str(),
                        observed.as_ref().map_or("nothing", GitOid::as_str)
                    ),
                )?;
            }
            AfterFastForward::Stop => return Err(refused),
        }
    }
}

/// Run `work`, the land step `step` under `turn`, to its end; should the turn be lost meanwhile,
/// the land stops once the step has ended and never goes on to the next, and a turn already lost
/// starts no step: not knowing whether another land runs is no permission to run beside it
/// (05_gateway.md "Land turns").
async fn holding<T>(
    turn: &mut Turn,
    name: &cowshed_core::api::dto::LandTurnName,
    step: &str,
    work: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if let Some(lost) = lost_now(turn).await {
        return Err(turn_lost(name, &format!("before {step}"), &lost));
    }
    let mut work = std::pin::pin!(work);
    let lost = tokio::select! {
        biased;
        lost = turn.lost() => lost,
        done = &mut work => return done,
    };
    let ended = match work.await {
        Ok(_) => "it completed".to_owned(),
        Err(error) => format!("it failed: {}", error.message),
    };
    Err(turn_lost(
        name,
        &format!("during {step}, which ran to its end ({ended})"),
        &lost,
    ))
}

/// The turn's loss, when the gateway has already closed its connection: polled once, never awaited.
async fn lost_now(turn: &mut Turn) -> Option<std::io::Error> {
    tokio::select! {
        biased;
        lost = turn.lost() => Some(lost),
        () = std::future::ready(()) => None,
    }
}

fn turn_lost(
    name: &cowshed_core::api::dto::LandTurnName,
    when: &str,
    lost: &std::io::Error,
) -> CowshedError {
    CowshedError::environment_missing(
        format!("land turn {name} was lost {when}: {lost}; the land stops before its next step"),
        "check the host gateway (`cowshed gateway status`), then land again",
    )
}

/// Rebase `workspace` onto main's head, again whenever main moved before the rebase began.
/// Answers that head and the rebased head.
async fn rebase<S, W, E>(
    service: &mut S,
    main: &Path,
    workspace: &str,
    output: &mut Output<W, E>,
) -> Result<(GitOid, GitOid)>
where
    S: CliService,
    W: Write + Send,
    E: Write + Send,
{
    loop {
        let base = main_head(main)?;
        let rebased = service
            .rebase(
                workspace,
                None,
                RebaseOptions {
                    expected_onto_head: Some(base.clone()),
                    ..RebaseOptions::default()
                },
            )
            .await;
        match rebased {
            Ok(report) => return Ok((base, report.oid().clone())),
            Err(error) if declared::rebase_again(error.fence_source()) => {
                say(
                    output,
                    &format!(
                        "main moved past {} before the rebase began; rebasing onto its new head",
                        base.as_str()
                    ),
                )?;
            }
            Err(error) => {
                if matches!(
                    error.fence_source(),
                    Some(FenceRefusal::ReplayStopped { .. })
                ) {
                    // A conflict leaves the replay stopped, and a land leaves its workspace as it
                    // was: the stopped replay is aborted before the conflict is reported.
                    let abort = RebaseOptions {
                        resume: Some(RebaseResume::Abort),
                        ..RebaseOptions::default()
                    };
                    if let Err(failure) = service.rebase(workspace, None, abort).await {
                        say(
                            output,
                            &format!(
                                "could not abort the stopped rebase ({}); clear it with cowshed rebase {workspace} --abort",
                                failure.message
                            ),
                        )?;
                    }
                }
                return Err(error);
            }
        }
    }
}

/// Run the declared gate bound to the land turn: it binds itself to the turn as it starts
/// ([`Turn::bind_at_exec`]), so the gateway holds the turn until the gate has exited however this
/// process ends, and a land killed before its spawn started no gate (02_workspaces.md "A declared
/// land" step 2). Its exit status is the land's verdict. A gate the gateway does not bind, or whose
/// turn is lost while it runs, is sent SIGTERM and stops the land once it has ended, so no gate
/// runs beside the next land.
async fn gate(
    main: &Path,
    declaration: &LandDeclaration,
    workspace: &str,
    environment: &[(OsString, OsString)],
    turn: &mut Turn,
) -> Result<()> {
    let argv = declaration.gate().argv(workspace);
    let mut command = declared_command(main, &argv, environment)?;
    // The gate's foreground execs end with this land (05_gateway.md, "A job lives with its
    // requester").
    let lander = crate::lander::Lander::this().map_err(|error| {
        CowshedError::internal(format!("cannot name this land to its gate: {error}"))
    })?;
    command.env(crate::lander::VARIABLE, lander.to_string());
    command
        .stdin(Stdio::null())
        .stdout(stderr()?)
        .stderr(stderr()?);
    let shown = shown(&argv);
    let _bind_guard = turn
        .prepare_bind()
        .await
        .map_err(|loss| turn_lost(declaration.turn(), "before the gate", &loss))?;
    turn.bind_at_exec(&mut command);
    let mut child = match tokio::process::Command::from(command).spawn() {
        Ok(child) => child,
        Err(error) => {
            drop(_bind_guard);
            if let Some(loss) = turn.loss() {
                return Err(CowshedError::environment_missing(
                    format!("land turn {} was lost: {loss}", declaration.turn()),
                    "check the host gateway (`cowshed gateway status`), then land again",
                ));
            }
            return Err(crate::land_turn::spawn_error(
                declaration.gate().program(),
                &error,
            ));
        }
    };
    let pid = crate::land_turn::spawned_pid(&child);
    let bound = turn.bound(pid.unsigned_abs()).await;
    drop(_bind_guard);
    // WHY unbounded: the gate is the land's own work and runs as long as it does; the turn's loss
    // ends the wait, and the gate is then sent SIGTERM and reaped within the reap bound.
    #[allow(clippy::disallowed_methods)]
    let held = match bound {
        Ok(()) => tokio::select! {
            status = child.wait() => Ok(status),
            error = turn.lost() => Err(format!("land turn {} was lost: {error}", declaration.turn())),
        },
        Err(error) => Err(format!(
            "the gateway did not bind it to land turn {}: {error}",
            declaration.turn()
        )),
    };
    let status = match held {
        Ok(status) => status,
        Err(reason) => {
            let stopped = crate::land_turn::forward(pid, libc::SIGTERM, &shown);
            let ended = format!(
                "; it {}",
                crate::land_turn::reap_stopped("declared gate", &mut child, stopped.is_ok()).await
            );
            let signal = match stopped {
                Ok(()) => "was sent SIGTERM".to_owned(),
                Err(error) => format!("was not reached by SIGTERM ({})", error.message),
            };
            return Err(CowshedError::environment_missing(
                format!(
                    "the gate `{shown}` {signal}: {reason}{ended}; the land stops and {workspace} stays as it is"
                ),
                "check the host gateway (`cowshed gateway status`), then land again",
            ));
        }
    }
    .map_err(|error| {
        CowshedError::internal(format!("cannot wait for the gate {shown}: {error}"))
    })?;
    if status.success() {
        return Ok(());
    }
    Err(CowshedError::new(
        ErrorCode::Conflict,
        format!(
            "the declared gate `{shown}` {}; the land stops and {workspace} stays as it is",
            describe(status)
        ),
        format!("fix what the gate refused in {workspace}, commit, then cowshed land {workspace}"),
    ))
}

/// Hand step 5 to the installed cowshed build as a bound child of `turn` after a mid-gate build
/// upgrade (02_workspaces.md "A declared land" step 5, 05_gateway.md "A land step waits for an
/// install in progress"). Passes the recorded proof explicitly; never re-runs the gate; fails
/// closed through the lost-turn path if the installed build refuses the proof format or exits
/// non-zero.
#[allow(clippy::too_many_arguments)]
async fn handoff_step_5<S: CliService>(
    service: &S,
    main: &Path,
    declaration: &LandDeclaration,
    land: &DeclaredLand<'_>,
    branch: &str,
    base: GitOid,
    head: GitOid,
    turn: &mut Turn,
) -> Result<Routed> {
    use tokio::io::AsyncReadExt as _;

    let program = service.background_program()?.ok_or_else(|| {
        turn_lost(
            declaration.turn(),
            "before step 5 hand-off",
            &std::io::Error::other(
                "this store names no installed cowshed executable for step 5 hand-off",
            ),
        )
    })?;
    let mut command = std::process::Command::new(&program);
    command
        .arg(STEP5_VERB)
        .args(["--proof-version", STEP5_PROOF_VERSION])
        .args(["--workspace", land.workspace])
        .args(["--source-head", head.as_str()])
        .args(["--target-head", base.as_str()])
        .args(["--ref-turn", declaration.ref_turn().as_str()])
        .args(["--verdict", "passed"]);
    if !land.retire {
        command.arg("--no-retire");
    }
    command
        .current_dir(main)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let _bind_guard = turn
        .prepare_bind()
        .await
        .map_err(|lost| turn_lost(declaration.turn(), "before step 5 hand-off", &lost))?;
    turn.bind_at_exec(&mut command);
    let mut child = match tokio::process::Command::from(command).spawn() {
        Ok(child) => child,
        Err(error) => {
            drop(_bind_guard);
            if let Some(loss) = turn.loss() {
                return Err(turn_lost(
                    declaration.turn(),
                    "starting step 5 hand-off",
                    &loss,
                ));
            }
            return Err(turn_lost(
                declaration.turn(),
                "starting step 5 hand-off",
                &error,
            ));
        }
    };
    let pid = crate::land_turn::spawned_pid(&child);
    let bound = turn.bound(pid.unsigned_abs()).await;
    drop(_bind_guard);
    let program_shown = program.display().to_string();
    if let Err(error) = bound {
        let stopped = crate::land_turn::forward(pid, libc::SIGTERM, &program_shown);
        let ended =
            crate::land_turn::reap_stopped("step 5 hand-off", &mut child, stopped.is_ok()).await;
        return Err(turn_lost(
            declaration.turn(),
            &format!("binding step 5 hand-off ({ended})"),
            &error,
        ));
    }

    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let read_stdout = async {
        let mut buf = Vec::new();
        if let Some(pipe) = stdout_pipe.as_mut() {
            let _ = pipe.read_to_end(&mut buf).await;
        }
        buf
    };
    let read_stderr = async {
        let mut buf = Vec::new();
        if let Some(pipe) = stderr_pipe.as_mut() {
            let _ = pipe.read_to_end(&mut buf).await;
        }
        buf
    };
    // WHY unbounded: step 5 runs as a bound child of the held turn; losing the turn ends the wait,
    // sends SIGTERM to the child, and reaps it within REAP.
    #[allow(clippy::disallowed_methods)]
    let wait_child = async {
        let (status, out, err) = tokio::join!(child.wait(), read_stdout, read_stderr);
        (status, out, err)
    };
    let outcome = tokio::select! {
        result = wait_child => Ok(result),
        lost = turn.lost() => Err(lost),
    };
    let (status, stdout, stderr) = match outcome {
        Ok(done) => done,
        Err(lost) => {
            let stopped = crate::land_turn::forward(pid, libc::SIGTERM, &program_shown);
            let ended =
                crate::land_turn::reap_stopped("step 5 hand-off", &mut child, stopped.is_ok())
                    .await;
            return Err(turn_lost(
                declaration.turn(),
                &format!("during step 5 hand-off ({ended})"),
                &lost,
            ));
        }
    };
    let status = status
        .map_err(|error| turn_lost(declaration.turn(), "waiting for step 5 hand-off", &error))?;
    if !status.success() {
        let detail = String::from_utf8_lossy(&stderr).trim().to_owned();
        let reason = if detail.is_empty() {
            format!(
                "the installed cowshed ({program_shown}) refused step 5 hand-off ({})",
                describe(status)
            )
        } else {
            format!(
                "the installed cowshed ({program_shown}) refused step 5 hand-off ({}): {detail}",
                describe(status)
            )
        };
        return Err(turn_lost(
            declaration.turn(),
            "during step 5 hand-off",
            &std::io::Error::other(reason),
        ));
    }
    let landed: LandedReport = if !stdout.is_empty() {
        serde_json::from_slice(&stdout).unwrap_or_else(|_| LandedReport {
            landed_head: head.clone(),
            target_branch: branch.to_owned(),
            previous_target_head: Some(base.clone()),
            target_was_checked_out: true,
            retired: false,
            build_volume: LandBuildVolume {
                seed: LandSeed::Unchanged,
                adoption: Adoption::Skipped {
                    reason: AdoptionSkip::NoLandingVolume,
                },
            },
            folded: Vec::new(),
        })
    } else {
        LandedReport {
            landed_head: head.clone(),
            target_branch: branch.to_owned(),
            previous_target_head: Some(base.clone()),
            target_was_checked_out: true,
            retired: false,
            build_volume: LandBuildVolume {
                seed: LandSeed::Unchanged,
                adoption: Adoption::Skipped {
                    reason: AdoptionSkip::NoLandingVolume,
                },
            },
            folded: Vec::new(),
        }
    };
    Ok(Routed {
        head: landed.landed_head.clone(),
        branch: landed.target_branch.clone(),
        before: base,
        retire: Ok(landed),
    })
}

/// Dispatch `<cowshed> __land-step5 ...` when this process is an installed build invoked by an
/// older build's declared land after a mid-gate gateway restart (02_workspaces.md "A declared
/// land" step 5). Returns `Some(exit_code)` when handled, `None` otherwise.
pub fn dispatch_step5() -> Option<i32> {
    let mut args = std::env::args_os();
    let _program = args.next()?;
    if args.next()? != STEP5_VERB {
        return None;
    }
    let mut proof_version: Option<String> = None;
    let mut workspace: Option<String> = None;
    let mut source_head: Option<String> = None;
    let mut target_head: Option<String> = None;
    let mut ref_turn: Option<String> = None;
    let mut verdict: Option<String> = None;
    let mut retire = true;
    while let Some(flag) = args.next() {
        let Some(flag_str) = flag.to_str() else {
            cowshed_os::eprintln!("refused: non-UTF-8 argument in {STEP5_VERB}");
            return Some(2);
        };
        match flag_str {
            "--no-retire" => retire = false,
            "--proof-version" => proof_version = args.next().and_then(|v| v.into_string().ok()),
            "--workspace" => workspace = args.next().and_then(|v| v.into_string().ok()),
            "--source-head" => source_head = args.next().and_then(|v| v.into_string().ok()),
            "--target-head" => target_head = args.next().and_then(|v| v.into_string().ok()),
            "--ref-turn" => ref_turn = args.next().and_then(|v| v.into_string().ok()),
            "--verdict" => verdict = args.next().and_then(|v| v.into_string().ok()),
            other => {
                cowshed_os::eprintln!("refused: unsupported step-5 proof format option {other}");
                return Some(2);
            }
        }
    }
    match proof_version.as_deref() {
        Some(STEP5_PROOF_VERSION) => {}
        Some(other) => {
            cowshed_os::eprintln!("refused: unsupported step-5 proof format v{other}");
            return Some(2);
        }
        None => {
            cowshed_os::eprintln!("refused: missing step-5 proof format version");
            return Some(2);
        }
    }
    if verdict.as_deref() != Some("passed") {
        cowshed_os::eprintln!(
            "refused: step-5 proof format requires --verdict passed, got {verdict:?}"
        );
        return Some(2);
    }
    let (Some(workspace), Some(source_head), Some(target_head), Some(ref_turn)) =
        (workspace, source_head, target_head, ref_turn)
    else {
        cowshed_os::eprintln!("refused: incomplete step-5 proof format arguments");
        return Some(2);
    };
    let source_head = match GitOid::new(source_head) {
        Ok(oid) => oid,
        Err(error) => {
            cowshed_os::eprintln!("refused: invalid step-5 proof format source head: {error}");
            return Some(2);
        }
    };
    let target_head = match GitOid::new(target_head) {
        Ok(oid) => oid,
        Err(error) => {
            cowshed_os::eprintln!("refused: invalid step-5 proof format target head: {error}");
            return Some(2);
        }
    };
    let ref_turn = match LandTurnName::new(ref_turn) {
        Ok(name) => name,
        Err(error) => {
            cowshed_os::eprintln!("refused: invalid step-5 proof format ref turn: {error}");
            return Some(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            cowshed_os::eprintln!("cowshed: cannot start runtime for {STEP5_VERB}: {error}");
            return Some(1);
        }
    };
    match runtime.block_on(crate::runtime::run_step5_handoff(
        &workspace,
        source_head,
        target_head,
        ref_turn,
        retire,
    )) {
        Ok(landed) => {
            if let Ok(json) = serde_json::to_vec(&landed) {
                let _ = std::io::stdout().write_all(&json);
            }
            Some(0)
        }
        Err(error) => {
            cowshed_os::eprintln!("cowshed: {STEP5_VERB}: {}", error.message);
            Some(1)
        }
    }
}

/// A declared command in `main`'s pure environment: `direnv exec <main> <argv…>` when main has
/// an `.envrc`, `<argv…>` itself otherwise, in main's checkout, with the held-command policies.
pub(crate) fn declared_command(
    main: &Path,
    argv: &[OsString],
    environment: &[(OsString, OsString)],
) -> Result<std::process::Command> {
    let Some((program, rest)) = argv.split_first() else {
        return Err(CowshedError::internal(
            "a declared command reached its spawn empty",
        ));
    };
    let mut command = if main.join(".envrc").exists() {
        let mut command = std::process::Command::new("direnv");
        command.arg("exec").arg(main).arg(program);
        command
    } else {
        std::process::Command::new(program)
    };
    command
        .args(rest)
        .current_dir(main)
        .env_clear()
        .envs(environment.iter().map(|(name, value)| (name, value)));
    let held =
        cowshed_core::held_lock::held_command_environment(None).map_err(CowshedError::internal)?;
    command.envs(held.iter().map(|(name, value)| (&**name, &**value)));
    Ok(command)
}

/// The pure environment of a caller whose identity variables `caller` answers: that identity and
/// the host bootstrap `PATH`.
pub(crate) fn pure_environment(
    caller: impl Fn(&str) -> Option<OsString>,
) -> Result<Vec<(OsString, OsString)>> {
    let home = cowshed_core::gateway_sessions::canonical_home()?;
    let path = std::env::join_paths(declared::bootstrap_path(&home)).map_err(|error| {
        CowshedError::internal(format!("cannot build the bootstrap PATH: {error}"))
    })?;
    Ok(declared::pure_environment(caller, path))
}

/// Step 3 starts from the push remote's head (02_workspaces.md "A declared land"). A writer that
/// pushes main without landing it, such as a release workflow's version commits, can leave the
/// remote ahead of this checkout's main, and a land onto the main it left behind would gate, land
/// and run its after-land commands on a head its push then cannot fast-forward. So main is brought
/// to the remote's branch first when that is ahead, under the ref turn, as every writer of main's
/// ref is. A main that holds the remote's head is landed onto as it is, and one that has diverged
/// from it is refused, naming both heads, before anything moves. A remote that cannot be read, or
/// that has no such branch yet, costs only this step: a land never needs its remote, so it says
/// so and lands onto main as it is, and its push is recorded like any other.
async fn sync_with_push_remote<S, W, E>(
    service: &S,
    main: &Path,
    declaration: &LandDeclaration,
    workspace: &str,
    branch: &str,
    output: &mut Output<W, E>,
) -> Result<()>
where
    S: CliService,
    W: Write + Send,
    E: Write + Send,
{
    let Some(remote) = declaration.push() else {
        return Ok(());
    };
    let remote = remote.as_str();
    let theirs = match remote_head(main, remote, branch).await {
        Ok(Some(head)) => head,
        Ok(None) => return Ok(()),
        Err(reason) => {
            return say(
                output,
                &format!(
                    "cannot read {remote}'s {branch} ({reason}); landing onto main as it is, so its push may be refused"
                ),
            );
        }
    };
    let ours = main_head(main)?;
    if ours == theirs || main_holds(main, &theirs)? {
        return Ok(());
    }
    if !holds(main, &theirs, &ours)? {
        return Err(CowshedError::new(
            ErrorCode::Conflict,
            format!(
                "main is at {} and {remote}'s {branch} at {}, and neither holds the other: a land onto this main would push a head {remote} refuses",
                ours.as_str(),
                theirs.as_str()
            ),
            format!("reconcile main with {remote}'s {branch}, then cowshed land {workspace}"),
        ));
    }
    let gateway = cowshed_core::land_turn::TurnGateway::host(service.control_socket()?)?;
    let mut turn = RefTurn::acquire_in(
        &gateway,
        declaration.ref_turn(),
        RefWriter::Land { workspace },
    )
    .await?;
    let mut command = std::process::Command::new("git");
    command
        .arg("-C")
        .arg(main)
        .args(["merge", "--ff-only", "--quiet", theirs.as_str()]);
    let ran = crate::doc_commit::run_split(command, None, Some(&mut turn)).await;
    let moved = matches!(&ran, Ok((status, _, _)) if status.success());
    let _ = turn.end(if moved {
        format!("fast-forwarded main to {}", theirs.as_str())
    } else {
        "refused: main was not fast-forwarded".to_owned()
    });
    let (status, _, stderr) = ran?;
    if !status.success() {
        return Err(CowshedError::new(
            ErrorCode::Conflict,
            format!(
                "git merge --ff-only {} in main {}: {}",
                theirs.as_str(),
                describe(status),
                String::from_utf8_lossy(&stderr).trim()
            ),
            format!("bring main to {remote}'s {branch}, then cowshed land {workspace}"),
        ));
    }
    say(
        output,
        &format!(
            "main was behind {remote}'s {branch}: fast-forwarded it from {} to {} before rebasing {workspace}",
            ours.as_str(),
            theirs.as_str()
        ),
    )
}

/// The head of `remote`'s `branch`, fetched into main's repository; `None` when the remote has no
/// such branch. Git aborts a transfer that stalls and never prompts, so an unreachable remote
/// answers an error instead of holding the land.
async fn remote_head(
    main: &Path,
    remote: &str,
    branch: &str,
) -> std::result::Result<Option<GitOid>, String> {
    let git = |args: &[&str]| {
        let mut command = std::process::Command::new("git");
        command
            .arg("-C")
            .arg(main)
            .args(["-c", "http.lowSpeedLimit=1", "-c", "http.lowSpeedTime=60"])
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0");
        command
    };
    let refname = format!("refs/heads/{branch}");
    let (status, listed, stderr) =
        crate::doc_commit::run_split(git(&["ls-remote", remote, &refname]), None, None)
            .await
            .map_err(|error| error.message)?;
    if !status.success() {
        return Err(String::from_utf8_lossy(&stderr).trim().to_owned());
    }
    if listed.is_empty() {
        return Ok(None);
    }
    let (status, _, stderr) = crate::doc_commit::run_split(
        git(&["fetch", "--quiet", "--no-tags", remote, &refname]),
        None,
        None,
    )
    .await
    .map_err(|error| error.message)?;
    if !status.success() {
        return Err(String::from_utf8_lossy(&stderr).trim().to_owned());
    }
    let (status, head, stderr) = crate::doc_commit::run_split(
        git(&["rev-parse", "--verify", "FETCH_HEAD^{commit}"]),
        None,
        None,
    )
    .await
    .map_err(|error| error.message)?;
    if !status.success() {
        return Err(String::from_utf8_lossy(&stderr).trim().to_owned());
    }
    GitOid::new(String::from_utf8_lossy(&head).trim().to_owned())
        .map(Some)
        .map_err(|error| format!("FETCH_HEAD is not an object id: {error}"))
}

/// Whether `descendant` holds `ancestor` (`git merge-base --is-ancestor`), both in main's repository.
fn holds(main: &Path, descendant: &GitOid, ancestor: &GitOid) -> Result<bool> {
    // WHY unbounded: a git read of this host's own main checkout: it runs no hook and waits on no
    // other process.
    #[allow(clippy::disallowed_methods)]
    let status = std::process::Command::new("git")
        .arg("-C")
        .arg(main)
        .args([
            "merge-base",
            "--is-ancestor",
            ancestor.as_str(),
            descendant.as_str(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output_locked()
        .map(|output| output.status)
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot run git to read whether {} holds {}: {error}",
                    descendant.as_str(),
                    ancestor.as_str()
                ),
                "install git",
            )
        })?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(CowshedError::internal(format!(
            "git merge-base --is-ancestor {} {} in {} {}",
            ancestor.as_str(),
            descendant.as_str(),
            main.display(),
            describe(status)
        ))),
    }
}

/// Whether main's head holds `commit` (`git merge-base --is-ancestor`); a commit main's repository
/// does not have (the workspace's own, never delivered) it does not hold.
fn main_holds(main: &Path, commit: &GitOid) -> Result<bool> {
    // WHY unbounded: a git read of this host's own main checkout: it runs no hook and waits on no
    // other process.
    #[allow(clippy::disallowed_methods)]
    let status = std::process::Command::new("git")
        .arg("-C")
        .arg(main)
        .args(["merge-base", "--is-ancestor", commit.as_str(), "HEAD"])
        .env("GIT_NO_LAZY_FETCH", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output_locked()
        .map(|output| output.status)
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot run git to read whether main holds {}: {error}",
                    commit.as_str()
                ),
                "install git",
            )
        })?;
    // WHY unbounded: a git read of this host's own main checkout: it runs no hook and waits on no
    // other process.
    #[allow(clippy::disallowed_methods)]
    let present = std::process::Command::new("git")
        .arg("-C")
        .arg(main)
        .args(["cat-file", "-e", &format!("{}^{{commit}}", commit.as_str())])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output_locked()
        .is_ok_and(|output| output.status.success());
    if !present {
        return Ok(false);
    }
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(CowshedError::internal(format!(
            "git merge-base --is-ancestor {} HEAD in {} {}",
            commit.as_str(),
            main.display(),
            describe(status)
        ))),
    }
}

/// Main's head, read by git in main's checkout.
pub(crate) fn main_head(main: &Path) -> Result<GitOid> {
    // WHY unbounded: a git read of this host's own main checkout: it runs no hook and waits on no
    // other process.
    #[allow(clippy::disallowed_methods)]
    let read = std::process::Command::new("git")
        .arg("-C")
        .arg(main)
        .args(["rev-parse", "--verify", "HEAD"])
        .stdin(Stdio::null())
        .output_locked()
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot run git to read main's head: {error}"),
                "install git",
            )
        })?;
    if !read.status.success() {
        return Err(CowshedError::internal(format!(
            "git cannot read main's head in {}: {}",
            main.display(),
            String::from_utf8_lossy(&read.stderr).trim()
        )));
    }
    GitOid::new(String::from_utf8_lossy(&read.stdout).trim().to_owned()).map_err(|error| {
        CowshedError::internal(format!("main's head is not an object id: {error}"))
    })
}

/// This process's stderr for a child, so a declared command's output reaches the land's reader
/// on the stream the land's own progress uses; stdout stays the land's result.
fn stderr() -> Result<Stdio> {
    std::io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .map(Stdio::from)
        .map_err(|error| {
            CowshedError::internal(format!(
                "cannot share stderr with a declared command: {error}"
            ))
        })
}

pub(crate) fn shown(argv: &[OsString]) -> String {
    argv.iter()
        .map(|word| word.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn describe(status: std::process::ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt as _;
    match (status.code(), status.signal()) {
        (Some(code), _) => format!("exited {code}"),
        (None, Some(signal)) => format!("was killed by signal {signal}"),
        (None, None) => "ended with neither an exit code nor a signal".to_owned(),
    }
}

fn say<W: Write + Send, E: Write + Send>(output: &mut Output<W, E>, message: &str) -> Result<()> {
    output
        .guidance(&format!("land: {message}"))
        .map_err(output_error)
}

/// Record `record` in the turn's `lands.log`; a line that cannot be written is said and never
/// stops the land.
fn note<W: Write + Send, E: Write + Send>(
    output: &mut Output<W, E>,
    directory: &Path,
    record: LandRecord,
) -> Result<()> {
    if let Err(error) = declared::record(directory, &record) {
        output
            .warning(&format!(
                "cannot record {:?} for {} in {}: {error}",
                record.event,
                record.head.as_str(),
                directory.join(declared::LANDS_LOG).display()
            ))
            .map_err(output_error)?;
    }
    Ok(())
}

/// Where a turn's after-land output is kept.
pub(crate) fn after_directory(turn: &Path) -> PathBuf {
    turn.join("lands")
}

#[cfg(test)]
mod tests {
    /// An after-land job a daemon of another build refused — one an install restarted the gateway
    /// onto after the land's turn ended — is recorded naming both builds and the command that runs
    /// it by hand as the installed cowshed, the job's own argv quoted for a shell.
    #[test]
    fn a_refused_after_job_names_the_builds_and_its_rerun() {
        use cowshed_core::error::OtherBuild;
        let refused = cowshed_core::CowshedError::other_build(OtherBuild {
            daemon: serde_json::from_value(serde_json::json!("installed-build")).unwrap(),
            caller: Some(serde_json::from_value(serde_json::json!("landing-build")).unwrap()),
        });
        let rerun = [
            "/Users/someone/Library/Application Support/dev.cowshed/bin/cowshed",
            "__land-after",
            "/tmp/lands/head.json",
        ]
        .map(super::shell_word)
        .join(" ");
        assert_eq!(
            super::not_admitted(&refused, &rerun),
            "the cowshed daemon is build installed-build; this cowshed is build landing-build; \
             run it by hand: '/Users/someone/Library/Application Support/dev.cowshed/bin/cowshed' \
             __land-after /tmp/lands/head.json"
        );
    }

    fn after_fixture(
        label: &str,
    ) -> (
        crate::temp_root::TempRoot,
        std::path::PathBuf,
        super::AfterRequest,
    ) {
        use cowshed_core::api::dto::GitOid;
        use cowshed_core::declared_land::RemoteName;
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            &format!("cs-after-{label}"),
        );
        let support = scratch.join("support");
        let bin = support.join("bin");
        std::fs::create_dir_all(&bin).expect("create support bin");
        let old = format!("cowshed-{}", "a".repeat(64));
        std::fs::write(bin.join(&old), b"#!/bin/sh\nexit 99\n").expect("write old copy");
        std::os::unix::fs::symlink(&old, bin.join(crate::launchd::COWSHED_BINARY_NAME))
            .expect("link stable name");
        let main = scratch.join("main");
        std::fs::create_dir_all(&main).expect("create main");
        let turn = scratch.join("turn");
        let request = super::AfterRequest {
            main,
            turn,
            branch: "main".to_owned(),
            before: GitOid::new("1".repeat(40)).expect("before oid"),
            head: GitOid::new("2".repeat(40)).expect("head oid"),
            push: Some(RemoteName::parse("origin".to_owned()).expect("remote")),
            after: vec![vec!["true".to_owned()]],
            environment: vec![
                ("HOME".to_owned(), scratch.display().to_string()),
                ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
            ],
        };
        (scratch, support, request)
    }

    fn replace_stable_with_script(support: &std::path::Path, script: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let bin = support.join("bin");
        let new_name = format!("cowshed-{}", "b".repeat(64));
        let new_path = bin.join(&new_name);
        std::fs::write(&new_path, script).expect("write new copy");
        std::fs::set_permissions(&new_path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod new copy");
        let stable = bin.join(crate::launchd::COWSHED_BINARY_NAME);
        std::fs::remove_file(&stable).expect("remove old stable symlink");
        std::os::unix::fs::symlink(&new_name, &stable).expect("link new stable name");
        new_path
    }

    fn other_build_refusal() -> cowshed_core::CowshedError {
        use cowshed_core::error::OtherBuild;
        cowshed_core::CowshedError::other_build(OtherBuild {
            daemon: serde_json::from_value(serde_json::json!("installed-build")).unwrap(),
            caller: Some(serde_json::from_value(serde_json::json!("landing-build")).unwrap()),
        })
    }

    fn read_lands(turn: &std::path::Path) -> Vec<super::LandRecord> {
        let path = turn.join(cowshed_core::declared_land::LANDS_LOG);
        if !path.exists() {
            return Vec::new();
        }
        std::fs::read_to_string(&path)
            .expect("read lands.log")
            .lines()
            .map(|line| serde_json::from_str(line).expect("parse lands.log record"))
            .collect()
    }

    /// Red: an after-land submit refused as another build after an install replaced the stable
    /// name goes on as the installed copy over the written request and returns the admitted job id.
    #[tokio::test]
    async fn an_after_job_refused_as_another_build_after_an_install_goes_on_as_the_installed_copy()
    {
        let (scratch, support, request) = after_fixture("admitted");
        let step = crate::install_basis::StepStart::at(&support);
        let stable = support
            .join("bin")
            .join(crate::launchd::COWSHED_BINARY_NAME);
        let argv_file = scratch.join("argv.txt");
        let env_file = scratch.join("env.txt");
        replace_stable_with_script(
            &support,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nenv > '{}'\nprintf '77\\n'\n",
                argv_file.display(),
                env_file.display(),
            ),
        );

        let started = super::start_after_with(&stable, &request, &step, |_| async {
            Err(other_build_refusal())
        })
        .await;
        assert_eq!(
            started,
            Ok(super::AdmittedAfter {
                job: 77,
                post_admission_exit: None,
            }),
            "installed copy must admit the after-land job"
        );

        let mut output = super::Output::new(Vec::new(), Vec::new(), false);
        super::record_after_outcome(&mut output, &request.turn, &request.head, true, started)
            .expect("record job line");
        let (_, stderr) = output.into_inner();
        let stderr = String::from_utf8_lossy(&stderr);
        assert!(
            stderr
                .contains("the push and after-land commands run as job 77 of main, never awaited"),
            "expected job line on stderr: {stderr}"
        );
        assert!(
            read_lands(&request.turn).is_empty(),
            "no push-failed record when installed copy admitted the job"
        );

        let request_path =
            super::after_directory(&request.turn).join(format!("{}.json", request.head.as_str()));
        assert!(request_path.is_file(), "request JSON must be written");
        let passed_argv: Vec<String> = std::fs::read_to_string(&argv_file)
            .expect("read recorded argv")
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(
            passed_argv,
            vec![
                "--project",
                &request.main.display().to_string(),
                "--quiet",
                "exec",
                "main",
                "--host",
                "--background",
                "--",
                &stable.display().to_string(),
                crate::land_after::VERB,
                &request_path.display().to_string(),
            ]
        );
        let passed_env = std::fs::read_to_string(&env_file).expect("read recorded env");
        assert!(
            passed_env
                .lines()
                .any(|line| line == format!("HOME={}", scratch.display()))
                && passed_env.lines().any(|line| line == "PATH=/usr/bin:/bin"),
            "installed copy must run with request.environment: {passed_env}"
        );
    }

    /// Control: a refusal of any other kind never invokes the installed copy, even if the stable
    /// name changed, and records `push-failed` with the by-hand command.
    #[tokio::test]
    async fn an_after_job_with_another_refusal_records_push_failed_without_handoff() {
        let (scratch, support, request) = after_fixture("other-err");
        let step = crate::install_basis::StepStart::at(&support);
        let stable = support
            .join("bin")
            .join(crate::launchd::COWSHED_BINARY_NAME);
        let invoked = scratch.join("invoked");
        replace_stable_with_script(
            &support,
            &format!(
                "#!/bin/sh\necho ran > '{}'\nprintf '77\\n'\n",
                invoked.display()
            ),
        );

        let started = super::start_after_with(&stable, &request, &step, |_| async {
            Err(cowshed_core::CowshedError::environment_missing(
                "daemon unreachable",
                "start gateway",
            ))
        })
        .await;
        assert!(
            started.is_err(),
            "non-other-build refusal must not hand off"
        );
        assert!(!invoked.exists(), "installed copy must not be invoked");

        let mut output = super::Output::new(Vec::new(), Vec::new(), false);
        super::record_after_outcome(&mut output, &request.turn, &request.head, true, started)
            .expect("record push-failed");
        let lands = read_lands(&request.turn);
        assert_eq!(lands.len(), 1);
        assert_eq!(lands[0].event, super::LandEvent::PushFailed);
        let detail = lands[0].detail.as_deref().expect("detail");
        assert!(
            detail.contains("daemon unreachable")
                && detail.contains("run it by hand:")
                && detail.contains(crate::land_after::VERB),
            "{detail}"
        );
    }

    /// Control: another-build refusal when the stable name was not replaced since the land began
    /// never invokes the installed copy and records `push-failed` with the by-hand command.
    #[tokio::test]
    async fn an_after_job_other_build_without_replaced_stable_name_records_push_failed() {
        let (scratch, support, request) = after_fixture("unreplaced");
        let step = crate::install_basis::StepStart::at(&support);
        let stable = support
            .join("bin")
            .join(crate::launchd::COWSHED_BINARY_NAME);
        let invoked = scratch.join("invoked");
        // Overwrite the target in place without moving the stable symlink so StepStart::replaced is false.
        let target = support
            .join("bin")
            .join(std::fs::read_link(&stable).expect("stable target"));
        std::fs::write(
            &target,
            format!(
                "#!/bin/sh\necho ran > '{}'\nprintf '77\\n'\n",
                invoked.display()
            ),
        )
        .expect("update target");

        let started = super::start_after_with(&stable, &request, &step, |_| async {
            Err(other_build_refusal())
        })
        .await;
        assert!(started.is_err(), "unreplaced stable name must not hand off");
        assert!(!invoked.exists(), "unchanged copy must not be invoked");

        let mut output = super::Output::new(Vec::new(), Vec::new(), false);
        super::record_after_outcome(&mut output, &request.turn, &request.head, true, started)
            .expect("record push-failed");
        let lands = read_lands(&request.turn);
        assert_eq!(lands.len(), 1);
        assert_eq!(lands[0].event, super::LandEvent::PushFailed);
        let detail = lands[0].detail.as_deref().expect("detail");
        assert!(
            detail.contains("installed-build")
                && detail.contains("run it by hand:")
                && detail.contains(crate::land_after::VERB),
            "{detail}"
        );
    }

    /// Control: when the installed copy itself fails (e.g., refused as another build again), the
    /// submit does not retry and records `push-failed` with the failure and by-hand command.
    #[tokio::test]
    async fn an_after_job_whose_installed_copy_fails_records_push_failed_with_rerun() {
        let (scratch, support, request) = after_fixture("installed-fails");
        let step = crate::install_basis::StepStart::at(&support);
        let stable = support
            .join("bin")
            .join(crate::launchd::COWSHED_BINARY_NAME);
        let count_file = scratch.join("calls.log");
        replace_stable_with_script(
            &support,
            &format!(
                "#!/bin/sh\necho call >> '{}'\necho 'the cowshed daemon is build third-build; this cowshed is build second-build' >&2\nexit 4\n",
                count_file.display(),
            ),
        );

        let started = super::start_after_with(&stable, &request, &step, |_| async {
            Err(other_build_refusal())
        })
        .await;
        assert!(started.is_err(), "failed installed copy must return Err");
        let calls = std::fs::read_to_string(&count_file)
            .expect("installed copy must have been invoked once");
        assert_eq!(
            calls.lines().count(),
            1,
            "must invoke installed copy once, never loop"
        );

        let mut output = super::Output::new(Vec::new(), Vec::new(), false);
        super::record_after_outcome(&mut output, &request.turn, &request.head, true, started)
            .expect("record push-failed");
        let lands = read_lands(&request.turn);
        assert_eq!(lands.len(), 1);
        assert_eq!(lands[0].event, super::LandEvent::PushFailed);
        let detail = lands[0].detail.as_deref().expect("detail");
        assert!(
            detail.contains("third-build")
                && detail.contains("run it by hand:")
                && detail.contains(crate::land_after::VERB),
            "{detail}"
        );
    }

    /// Red: if the installed copy prints a valid job id (proving admission) and then exits nonzero
    /// (e.g. a teardown failure in bridge.shutdown()), the land treats the job as admitted, records
    /// no push-failed, and warns with the exit status and stderr detail.
    #[tokio::test]
    async fn an_after_job_admitted_before_teardown_failure_returns_job_and_warns_without_push_failed()
     {
        let (_scratch, support, request) = after_fixture("teardown-fail");
        let step = crate::install_basis::StepStart::at(&support);
        let stable = support
            .join("bin")
            .join(crate::launchd::COWSHED_BINARY_NAME);
        replace_stable_with_script(
            &support,
            "#!/bin/sh\nprintf '77\\n'\necho 'bridge shutdown failed' >&2\nexit 1\n",
        );

        let started = super::start_after_with(&stable, &request, &step, |_| async {
            Err(other_build_refusal())
        })
        .await;
        let admitted = started
            .as_ref()
            .expect("valid job id on stdout proves admission despite nonzero teardown exit");
        assert_eq!(admitted.job, 77);
        let post_exit = admitted
            .post_admission_exit
            .as_ref()
            .expect("nonzero teardown exit must be preserved for caller warning");
        assert_eq!(post_exit.status, "exited 1");
        assert_eq!(post_exit.stderr, "bridge shutdown failed");
        let mut output = super::Output::new(Vec::new(), Vec::new(), false);
        super::record_after_outcome(&mut output, &request.turn, &request.head, true, started)
            .expect("record admitted job with teardown warning");
        let (_, stderr) = output.into_inner();
        let stderr = String::from_utf8_lossy(&stderr);
        assert!(
            stderr
                .contains("the push and after-land commands run as job 77 of main, never awaited"),
            "expected admitted job line on stderr: {stderr}"
        );
        assert!(
            stderr.contains("exited 1 after admitting job 77: bridge shutdown failed"),
            "expected post-admission exit warning on stderr: {stderr}"
        );
        assert!(
            read_lands(&request.turn).is_empty(),
            "must not record push-failed once the job id was admitted"
        );
        assert_eq!(super::parse_admitted_job_id("77\n"), Some(77));
        assert_eq!(super::parse_admitted_job_id("+77\n"), None);
        assert_eq!(super::parse_admitted_job_id("77\nextra\n"), None);
        assert_eq!(super::parse_admitted_job_id(""), None);
    }
}
