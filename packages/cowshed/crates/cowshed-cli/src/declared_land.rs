//! A declared land (02_workspaces.md "A declared land"): `cowshed land <ws>` on a repository whose
//! main `.cowshed.toml` declares `[land]` is the whole route from a committed shed to a pushed
//! main. This is its shell: it takes the turns, runs the declared commands and asks the service
//! for the rebase, the fast-forward and the retire. Every decision over what it observes is
//! [`cowshed_core::declared_land`]'s.

use std::ffi::OsString;
use std::io::Write;
use std::os::fd::{AsFd as _, RawFd};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use cowshed_core::api::dto::{
    ExpectedRefHead, GitOid, LandOptions, LandReport, LandedReport, RebaseOptions,
};
use cowshed_core::declared_land::{
    self as declared, AfterFastForward, AfterRequest, LandDeclaration, LandEvent, LandRecord,
};
use cowshed_core::{CowshedError, ErrorCode, Result};
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
    let turn = crate::land_turn::take_turn(
        turn_directory.clone(),
        declaration.turn().as_str(),
        land.workspace,
        format!("cowshed land {}", land.workspace),
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
        &turn.descriptors(),
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
        match start_after(service, &request) {
            Ok(log) => say(
                output,
                &format!(
                    "the push and after-land commands run detached, never awaited; their outcomes go to {} and their output to {}",
                    turn_directory.join(declared::LANDS_LOG).display(),
                    log.display()
                ),
            )?,
            Err(reason) => {
                let detail = format!(
                    "the after-land child did not start: {reason}; neither the push nor any after-land command ran"
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

/// Start the after-land child for `request`; why not, when it cannot start.
fn start_after<S: CliService>(
    service: &S,
    request: &AfterRequest,
) -> std::result::Result<PathBuf, String> {
    let program = service
        .background_program()
        .map_err(|error| error.message)?
        .ok_or_else(|| "this store names no cowshed executable for background work".to_owned())?;
    crate::land_after::start(&program, request).map_err(|error| error.message)
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
    descriptors: &[RawFd],
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
        let (base, head) = rebase(service, main, workspace, output).await?;
        say(
            output,
            &format!(
                "gating {} (rebased onto {}) with {}",
                head.as_str(),
                base.as_str(),
                declaration.gate().program()
            ),
        )?;
        gate(main, declaration, workspace, environment, descriptors).await?;
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
            Ok(report) => return Ok((base, report.oid)),
            Err(error) if declared::rebase_again(error.fence_source()) => {
                say(
                    output,
                    &format!(
                        "main moved past {} before the rebase began; rebasing onto its new head",
                        base.as_str()
                    ),
                )?;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Run the declared gate holding the turn's descriptors; its exit status is the land's verdict.
async fn gate(
    main: &Path,
    declaration: &LandDeclaration,
    workspace: &str,
    environment: &[(OsString, OsString)],
    descriptors: &[RawFd],
) -> Result<()> {
    let argv = declaration.gate().argv(workspace);
    let mut command = declared_command(main, &argv, environment)?;
    command
        .stdin(Stdio::null())
        .stdout(stderr()?)
        .stderr(stderr()?);
    cowshed_core::held_lock::hand_to(&mut command, descriptors.to_vec());
    let mut command = tokio::process::Command::from(command);
    let shown = shown(&argv);
    // WHY unbounded: the gate is the land's own work and runs as long as it does; it holds the turn's
    // descriptors, so no other land runs beside it.
    #[allow(clippy::disallowed_methods)]
    let status = command
        .spawn()
        .map_err(|error| crate::land_turn::spawn_error(declaration.gate().program(), &error))?
        .wait()
        .await
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
