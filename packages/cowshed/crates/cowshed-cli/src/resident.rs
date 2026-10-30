//! `path` and `exec` answered by a resident workspace (06_cli.md "Resident workspaces").
//!
//! A named workspace that is mounted, and whose records agree with its mount, is answered here
//! for `path` without opening the project controller and without asking its supervisor anything
//! ([`cowshed_core::resident::mounted`]): `path` runs no work there. `exec` is the core's resident
//! exec client, which the controller's exec runs too: the workspace's daemon-owned supervisor
//! serves its current authority ([`cowshed_core::resident::resolve`]), its gateway session is
//! current and its build state is what the controller's refresh would leave as it is
//! ([`Resident::admissible`]), and the job runs through the supervisor's socket on a
//! [`JobLink`]. This module only relays it. Everything else — and every workspace the resolution
//! declines — falls through to the controller untouched, so this path never answers what the
//! controller would answer differently.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use cowshed_core::api::{ExecRequest, JobId, JobInfo};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::resident::{Decline, HostProbe, JobLink, Mounted, Resident, mounted, resolve};
use cowshed_core::storage::bootstrap::CanonicalRoots;
use cowshed_core::storage::job_artifact::StreamKind;
use cowshed_core::{CowshedError, JobStream, Result};
use cowshed_gateway_types::STORE_ROOT;
use cowshed_os::timing::{event, span, spanned};
use tokio::io::AsyncRead;

use crate::args::{Cli, Command};
use crate::gateway_service::ControlSocket;
use crate::output::Output;
use crate::runtime::{
    DispatchExit, ExecEnd, ExecPresentation, ExecResult, ForegroundJob, emit_mount_path,
    exec_command, exec_presentation, hold_foreground, output_error, relay_foreground, report_exec,
    success,
};

/// Whether the resident path answered, and if not, what the controller path still needs.
pub enum Answer<R> {
    Answered(DispatchExit),
    /// Not resident: nothing was run, and `stdin` is untouched.
    Declined(R),
}

/// Answer `path` from a resident workspace when it names one; `None` when the controller has to.
pub fn answer_path<W, E>(cli: &Cli, output: &mut Output<W, E>) -> Result<Option<DispatchExit>>
where
    W: Write + Send,
    E: Write + Send,
{
    let Command::Path(args) = &cli.command else {
        return Ok(None);
    };
    let (Some(workspace), None) = (args.workspace.as_deref(), args.slot) else {
        return Ok(None);
    };
    let Some(mounted) = mounted_workspace(cli, workspace)? else {
        return Ok(None);
    };
    emit_mount_path(
        output,
        cli.global.json,
        &mounted.workspace,
        &mounted.mount,
        mounted.base_commit.as_ref(),
    )?;
    Ok(Some(success()))
}

/// Answer an `exec` from a resident workspace when it names one; otherwise hand `stdin` back.
pub async fn answer_exec<R, W, E>(
    cli: &Cli,
    stdin: R,
    output: &mut Output<W, E>,
) -> Result<Answer<R>>
where
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    let json = cli.global.json;
    let Command::Exec(args) = &cli.command else {
        return Ok(Answer::Declined(stdin));
    };
    if args.session.is_some() {
        return Ok(Answer::Declined(stdin));
    }
    let Some(resident) = resident(cli, &args.workspace).await? else {
        return Ok(Answer::Declined(stdin));
    };
    // The resident path reads the host's own store (`STORE_ROOT`), so the gateway it asks is the
    // host's.
    let admissible = match ControlSocket::at(CanonicalRoots::global().control_socket()) {
        Ok(gateway) => resident.admissible(&gateway).await,
        Err(_) => Err(Decline::GatewayUnanswered),
    };
    if let Err(decline) = admissible {
        declined(decline);
        return Ok(Answer::Declined(stdin));
    }
    let command = exec_command(args.clone(), stdin)?;
    let (stdout, stderr) = output.writers_mut();
    let result = run(
        resident,
        command.request,
        command.background,
        command.timeout,
        exec_presentation(json),
        stdout,
        stderr,
    )
    .await?;
    Ok(Answer::Answered(report_exec(
        output,
        json,
        &args.workspace,
        result,
    )?))
}

/// The mounted workspace `name` of the invocation's project, as `path` answers it, or `None`
/// when the controller has to answer.
fn mounted_workspace(cli: &Cli, name: &str) -> Result<Option<Mounted>> {
    let Ok(workspace) = WorkspaceName::new(name) else {
        return Ok(None);
    };
    let start = invocation_start(cli.global.project.as_deref())?;
    let _span = span("resident", "mounted");
    Ok(answered(
        mounted(Path::new(STORE_ROOT), &start, &workspace, &HostProbe).and_then(|mounted| {
            mounted.lifecycle_settled()?;
            Ok(mounted)
        }),
    ))
}

/// The resident workspace `name` of the invocation's project, as `exec` runs in it, or `None`
/// when the controller has to answer.
async fn resident(cli: &Cli, name: &str) -> Result<Option<Resident>> {
    let Ok(workspace) = WorkspaceName::new(name) else {
        return Ok(None);
    };
    let start = invocation_start(cli.global.project.as_deref())?;
    let resolved = spanned(
        "resident",
        "resolve",
        resolve(Path::new(STORE_ROOT), &start, &workspace, &HostProbe),
    )
    .await;
    Ok(answered(resolved))
}

/// `resolved`, or `None` once the reason it declined is said.
fn answered<T>(resolved: std::result::Result<T, Decline>) -> Option<T> {
    resolved.map_err(declined).ok()
}

/// The reason for a decline is a timing line, never output: the controller path gives the caller
/// the same answer either way.
fn declined(decline: Decline) {
    event("resident", || format!("declined: {}", decline.reason()));
}

/// Where project discovery starts: `--project` when given, resolved against the cwd as Git
/// resolves `git -C`, else the cwd itself.
fn invocation_start(project: Option<&Path>) -> Result<PathBuf> {
    let cwd = || {
        std::env::current_dir().map_err(|error| {
            CowshedError::environment_missing(
                format!("could not determine the current directory: {error}"),
                "use --project <git-root>",
            )
        })
    };
    match project {
        Some(path) if path.is_absolute() => Ok(path.to_path_buf()),
        Some(path) => Ok(cwd()?.join(path)),
        None => cwd(),
    }
}

/// Run one job in the resident workspace and relay it as the controller path does.
async fn run(
    resident: Resident,
    request: ExecRequest,
    background: bool,
    timeout: Option<Duration>,
    presentation: ExecPresentation,
    stdout: &mut (dyn Write + Send),
    stderr: &mut (dyn Write + Send),
) -> Result<ExecResult> {
    let build_volume = resident.build_volume.clone();
    let mut link = JobLink::new(&resident);
    // The CLI is the coordinator route (07_api.md, "CLI and API exec parity"), on this path as
    // through the controller: the route is who asks, never the class asked for.
    let source = cowshed_core::runtime::supervisor_socket::AdmissionSource::Coordinator;

    // Admission is where a refusal can still mean nothing ran; after it, every failure is this
    // job's and is reported, never retried through the controller.
    let job = spanned(
        "resident",
        "submit",
        link.handle().exec(None, build_volume, request, source),
    )
    .await?;
    if background {
        let info = link.info(job).await?;
        link.detach(job).await?;
        return Ok(ExecResult {
            info,
            end: ExecEnd::Backgrounded,
        });
    }
    let job = ResidentJob { link, job };
    // Held for as long as this process lives, as through the controller (05_gateway.md, "A job
    // lives with its requester").
    let held = cowshed_core::runtime::supervisor_socket::hold_under(
        job.link.socket(),
        job.link.handle().snapshot(),
        job.job,
    );
    let hold = hold_foreground(&job, held).await?;
    let _relay = span("resident", "relay");
    relay_foreground(&job, Some(hold), timeout, presentation, stdout, stderr).await
}

/// A job of the resident workspace, reached through its supervisor's socket.
struct ResidentJob {
    link: JobLink,
    job: JobId,
}

#[async_trait]
impl ForegroundJob for ResidentJob {
    async fn wait(&self) -> Result<JobInfo> {
        self.link.clone().wait(self.job).await
    }

    async fn status(&self) -> Result<JobInfo> {
        self.link.clone().info(self.job).await
    }

    /// The supervisor keeps a job running whether or not anyone is attached; detaching makes
    /// its captured output durable for whoever reads it later.
    async fn detach(&self) -> Result<()> {
        self.link.clone().detach(self.job).await
    }

    async fn kill(&self) -> Result<()> {
        self.link.clone().kill(self.job).await
    }

    /// Copy one stream to `writer` as it is produced, to its end.
    async fn relay(&self, stream: JobStream, writer: &mut (dyn Write + Send)) -> Result<()> {
        let stream = match stream {
            JobStream::Stdout => StreamKind::Stdout,
            JobStream::Stderr => StreamKind::Stderr,
        };
        let mut link = self.link.clone();
        let mut offset = 0_u64;
        loop {
            let chunk = link.log_read(self.job, stream, offset, true).await?;
            if !chunk.bytes.is_empty() {
                writer.write_all(&chunk.bytes).map_err(output_error)?;
                writer.flush().map_err(output_error)?;
            }
            offset = chunk.next_offset;
            if chunk.eof {
                return Ok(());
            }
        }
    }
}
