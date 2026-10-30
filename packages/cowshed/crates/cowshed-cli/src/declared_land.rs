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
    ExpectedRefHead, GitOid, LandOptions, LandReport, LandedReport, RebaseOptions, RebaseResume,
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

/// What a declared land was asked for beyond its workspace.
pub(crate) struct DeclaredLand<'a> {
    pub workspace: &'a str,
    pub retire: bool,
    pub push: bool,
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
        format!("cowshed land {} [--no-push] [--no-retire]", args.workspace),
    ))
}

/// Run the declared route for `land` in the repository whose main checkout is `main`.
pub(crate) async fn land<S, W, E>(
    service: &mut S,
    main: &Path,
    declaration: &LandDeclaration,
    land: DeclaredLand<'_>,
    output: &mut Output<W, E>,
) -> Result<LandReport>
where
    S: CliService,
    W: Write + Send,
    E: Write + Send,
{
    let root = service.land_turn_root()?;
    let turn_directory = root.join(declaration.turn().as_str());
    let environment = pure_environment()?;
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
        },
        None,
        output,
    )
    .await?;
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
            turn.end(match &routed.retire {
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
            turn.end(format!("refused: {}", error.message));
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
        match start_after(service, &request).await {
            Ok(job) => say(
                output,
                &format!(
                    "the push and after-land commands run as job {job} of main, never awaited; their outcomes go to {}",
                    turn_directory.join(declared::LANDS_LOG).display(),
                ),
            )?,
            Err(reason) => {
                let detail = format!(
                    "the after-land job was not admitted: {reason}; neither the push nor any after-land command ran"
                );
                note(
                    output,
                    &turn_directory,
                    LandRecord::now(
                        if pushing {
                            LandEvent::PushFailed
                        } else {
                            LandEvent::AfterExited
                        },
                        routed.head.clone(),
                        None,
                        Some(detail.clone()),
                    ),
                )?;
                output
                    .warning(&format!("landed {}, but {detail}", routed.head.as_str()))
                    .map_err(output_error)?;
            }
        }
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

/// Submit the after-land work for `request` to main's workspace supervisor as a background job at
/// the host boundary (02_workspaces.md "A declared land" step 6), and answer its job id without
/// waiting; why not, when it was not admitted.
async fn start_after<S: CliService>(
    service: &mut S,
    request: &AfterRequest,
) -> std::result::Result<u64, String> {
    let program = service
        .background_program()
        .map_err(|error| error.message)?
        .ok_or_else(|| "this store names no cowshed executable for background work".to_owned())?;
    let argv = crate::land_after::argv(&program, request).map_err(|error| error.message)?;
    let command = crate::runtime::ExecCommand {
        workspace: cowshed_core::git::MAIN_REMOTE.to_owned(),
        request: cowshed_core::api::ExecRequest {
            command: cowshed_core::api::ExecCommand::Argv(
                argv.into_iter()
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
            want: None,
            scope: cowshed_core::api::ExecScope::HostGate,
        },
        session: None,
        background: true,
        timeout: None,
    };
    let result = service
        .exec(
            command,
            crate::runtime::ExecPresentation::Control,
            &mut std::io::sink(),
            &mut std::io::sink(),
        )
        .await
        .map_err(|error| error.message)?;
    Ok(result.info.job_id.get())
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
            sync_with_push_remote(&*service, main, declaration, workspace, branch, output),
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
        if let Some(lost) = lost_now(turn).await {
            return Err(turn_lost(
                declaration.turn(),
                "before the fast-forward",
                &lost,
            ));
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
    command
        .stdin(Stdio::null())
        .stdout(stderr()?)
        .stderr(stderr()?);
    let shown = shown(&argv);
    turn.bind_at_exec(&mut command);
    let mut child = tokio::process::Command::from(command)
        .spawn()
        .map_err(|error| crate::land_turn::spawn_error(declaration.gate().program(), &error))?;
    let pid = crate::land_turn::spawned_pid(&child);
    // WHY unbounded: the gate is the land's own work and runs as long as it does; the turn's loss
    // ends the wait, and the gate is then sent SIGTERM and reaped within the reap bound.
    #[allow(clippy::disallowed_methods)]
    let held = match turn.bound(pid.unsigned_abs()).await {
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

/// The pure environment of this caller: its identity and the host bootstrap `PATH`.
pub(crate) fn pure_environment() -> Result<Vec<(OsString, OsString)>> {
    let home = cowshed_core::gateway_sessions::canonical_home()?;
    let path = std::env::join_paths(declared::bootstrap_path(&home)).map_err(|error| {
        CowshedError::internal(format!("cannot build the bootstrap PATH: {error}"))
    })?;
    Ok(declared::pure_environment(
        |name| std::env::var_os(name),
        path,
    ))
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
    let socket = service.control_socket()?;
    let mut turn = RefTurn::acquire_in(
        &socket,
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
    turn.end(if moved {
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
