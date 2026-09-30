//! `path` and `exec` answered by a resident workspace (06_cli.md "Resident workspaces").
//!
//! A named workspace that is mounted, whose daemon-owned supervisor serves its current authority,
//! and whose gateway session is current (for `exec`) is answered here without opening the
//! project controller: [`cowshed_core::resident::resolve`] reads its records and asks the live
//! state, and the job runs through the supervisor's socket. Everything else — and every
//! workspace the resolution declines — falls through to the controller untouched, so this path
//! never answers what the controller would answer differently.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use cowshed_core::api::{ExecRequest, JobId, JobInfo};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::resident::{Decline, HostProbe, Resident, resolve};
use cowshed_core::runtime::supervisor::WorkspaceSupervisorHandle;
use cowshed_core::storage::bootstrap::STORE_ROOT;
use cowshed_core::storage::job_artifact::StreamKind;
use cowshed_core::timing::{event, span, spanned};
use cowshed_core::{CowshedError, ErrorCode, JobStream, Result};
use tokio::io::AsyncRead;

use crate::args::{Cli, Command};
use crate::gateway_service::{
    ControlSocket, GatewayControl, control_socket_path, stable_workspace_id,
};
use crate::output::Output;
use crate::runtime::{
    DispatchExit, ExecEnd, ExecPresentation, ExecResult, ForegroundJob, emit_mount_path,
    exec_command, exec_presentation, output_error, relay_foreground, report_exec, success,
};

/// Whether the resident path answered, and if not, what the controller path still needs.
pub enum Answer<R> {
    Answered(DispatchExit),
    /// Not resident: nothing was run, and `stdin` is untouched.
    Declined(R),
}

/// Answer `cli` from a resident workspace when it names one; otherwise hand `stdin` back.
pub async fn answer<R, W, E>(cli: &Cli, stdin: R, output: &mut Output<W, E>) -> Result<Answer<R>>
where
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    let json = cli.global.json;
    match &cli.command {
        Command::Path(args) if args.slot.is_none() => {
            let Some(workspace) = args.workspace.as_deref() else {
                return Ok(Answer::Declined(stdin));
            };
            let Some(resident) = resident(cli, workspace).await? else {
                return Ok(Answer::Declined(stdin));
            };
            emit_mount_path(
                output,
                json,
                &resident.workspace,
                &resident.mount,
                resident.base_commit.as_ref(),
            )?;
            Ok(Answer::Answered(success()))
        }
        Command::Exec(args) if args.session.is_none() => {
            let Some(resident) = resident(cli, &args.workspace).await? else {
                return Ok(Answer::Declined(stdin));
            };
            if !gateway_current(&resident).await {
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
        _ => Ok(Answer::Declined(stdin)),
    }
}

/// The resident workspace `name` of the invocation's project, or `None` when the controller has
/// to answer. The reason for a decline is a timing line, never output: the controller path gives
/// the caller the same answer either way.
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
    Ok(match resolved {
        Ok(resident) => Some(resident),
        Err(decline) => {
            declined(decline);
            None
        }
    })
}

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

/// Whether the gateway holds this workspace's session at the revision its supervisor serves —
/// exactly the case in which the controller's pre-exec reconcile would install nothing for it.
async fn gateway_current(resident: &Resident) -> bool {
    let _span = span("resident", "gateway");
    let Ok(control) = ControlSocket::at(control_socket_path()) else {
        return false;
    };
    let Ok(status) = control.status().await else {
        return false;
    };
    let authority = resident.authority();
    let identity = stable_workspace_id(
        &resident.repo_id,
        resident.workspace.as_str(),
        &authority.workspace_incarnation,
    );
    let current = status.sessions.iter().any(|session| {
        session.workspace_id == identity && session.revision == authority.grant_revision
    });
    if !current {
        event("resident", || {
            "declined: the gateway session is not current".to_owned()
        });
    }
    current
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
    let link = JobLink::new(resident);
    // Admission is where a refusal can still mean nothing ran; after it, every failure is this
    // job's and is reported, never retried through the controller.
    let job = spanned("resident", "submit", link.handle.exec(None, request)).await?;
    if background {
        let info = link.clone().info(job).await?;
        return Ok(ExecResult {
            info,
            end: ExecEnd::Backgrounded,
        });
    }
    let _relay = span("resident", "relay");
    relay_foreground(
        &ResidentJob { link, job },
        timeout,
        presentation,
        stdout,
        stderr,
    )
    .await
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

    /// The supervisor keeps a job running whether or not anyone is attached, so there is
    /// nothing to tell it.
    async fn detach(&self) -> Result<()> {
        Ok(())
    }

    async fn relay(&self, stream: JobStream, writer: &mut (dyn Write + Send)) -> Result<()> {
        let stream = match stream {
            JobStream::Stdout => StreamKind::Stdout,
            JobStream::Stderr => StreamKind::Stderr,
        };
        self.link.clone().pump(self.job, stream, writer).await
    }
}

/// One lane to the job's supervisor. A grant change while the job runs advances the supervisor
/// in place, and it then refuses calls that name the authority it served before; the lane follows
/// it to the new one, which still holds the job.
#[derive(Clone)]
struct JobLink {
    socket: PathBuf,
    handle: WorkspaceSupervisorHandle,
}

impl JobLink {
    fn new(resident: Resident) -> Self {
        Self {
            socket: resident.socket().to_path_buf(),
            handle: resident.supervisor,
        }
    }

    async fn followed(&mut self, error: CowshedError) -> Result<()> {
        if error.code != ErrorCode::Conflict {
            return Err(error);
        }
        match cowshed_core::resident::follow(&self.socket, self.handle.snapshot(), &HostProbe)
            .await?
        {
            Some(handle) => {
                self.handle = handle;
                Ok(())
            }
            None => Err(error),
        }
    }

    async fn info(mut self, job: JobId) -> Result<JobInfo> {
        match self.handle.info(job).await {
            Err(error) => {
                self.followed(error).await?;
                self.handle.info(job).await
            }
            done => done,
        }
    }

    async fn wait(mut self, job: JobId) -> Result<JobInfo> {
        match self.handle.wait(job).await {
            Err(error) => {
                self.followed(error).await?;
                self.handle.wait(job).await
            }
            done => done,
        }
    }

    /// Copy one stream to `writer` as it is produced, to its end.
    async fn pump(
        mut self,
        job: JobId,
        stream: StreamKind,
        writer: &mut (dyn Write + Send),
    ) -> Result<()> {
        let mut offset = 0_u64;
        loop {
            let chunk = match self.handle.log_read(job, stream, offset, true).await {
                Err(error) => {
                    self.followed(error).await?;
                    continue;
                }
                Ok(chunk) => chunk,
            };
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
