//! A job's jobserver (05_gateway.md "Host scheduling", "Parallelism inside a job"). The kernel
//! schedules CPU among jobs; a jobserver bounds only how many compiler and test threads one
//! build tree starts, so a `cargo` or `nextest` does not hand the kernel ten times the host's
//! cores. Every job's command reads a GNU make jobserver fifo its supervisor serves with a
//! constant count: the host's cores, or the job's smaller want. Nothing here waits on another
//! job; the one wait is a background start's for host memory pressure to drop.
//!
//! The fifo is the job's own, in the workspace's pool directory ([`cowshed_types::paths::cpu_pool_directory`]), which
//! the job's sandbox opens for FIFOs alone (04_sandbox.md). The supervisor makes it mode `0600`,
//! keeps its own read-write descriptor, so the token bytes survive while no reader has it open,
//! writes `tokens − 1` bytes (the command holds the implicit first token, as make's protocol has
//! it), and unlinks it when the job concludes. A Cargo that outlives the job keeps its open
//! descriptor; one started after finds no fifo, runs unconfigured, and says so itself.

use std::collections::BTreeMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{self, Write as _};
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::api::dto::{ExecClass, JobId};
use crate::error::{CowshedError, CpuAdmissionRefusal, Result};
use crate::host_pressure::{Decision, RECHECK_EVERY, may_start, read_memory_pressure};

/// The count a job's command reads beside its fifo (05_gateway.md "Cargo reads its holder's
/// jobserver").
pub const CPU_TOKENS_ENV: &str = "BOUNDED_EXEC_CPU_TOKENS";

/// The make flags a jobserver exports to Cargo and make, ahead of its fifo's path.
pub(crate) const JOBSERVER_FIFO_FLAGS: &str = "-j --jobserver-auth=fifo:";

/// One token: any byte is one to make and Cargo.
const TOKEN_BYTE: u8 = b'+';

/// The host's cores: the count every jobserver holds unless its job wants fewer.
pub fn host_cores() -> NonZeroUsize {
    std::thread::available_parallelism().expect("every supported host reports how many CPUs it has")
}

/// The jobserver fifo `flags` name, when they are the ones a jobserver exports.
fn fifo_in_flags(flags: &OsStr) -> Option<&Path> {
    flags
        .as_bytes()
        .strip_prefix(JOBSERVER_FIFO_FLAGS.as_bytes())
        .filter(|fifo| !fifo.is_empty())
        .map(|fifo| Path::new(OsStr::from_bytes(fifo)))
}

/// The jobserver fifo a command's environment names, read back from the `CARGO_MAKEFLAGS` its
/// supervisor set. The exec host, the first process inside the sandbox, opens it before the
/// command runs and says on the job's stderr when it cannot.
pub fn granted_jobserver_fifo(environment: &BTreeMap<OsString, OsString>) -> Option<&Path> {
    fifo_in_flags(environment.get(OsStr::new("CARGO_MAKEFLAGS"))?)
}

/// The jobserver a requester's own job serves it: the fifo its command reads and that job's count.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InheritedJobserver {
    pub fifo: PathBuf,
    pub tokens: NonZeroUsize,
}

/// The jobserver this process inherited from the job that started it, if any: the fifo its
/// `CARGO_MAKEFLAGS` names beside the count its [`CPU_TOKENS_ENV`] names. The controller runs in
/// the requester's process (in-process for the CLI and every embedder; `cowshed controller`
/// inherits its embedder's environment), so a `cowshed exec` from inside a job names its parent's
/// jobserver, which the new job reads too when its sandbox can open it. A fifo beside no count, or
/// a count that is no positive count, names nothing.
pub fn requester_jobserver() -> Option<InheritedJobserver> {
    let fifo = fifo_in_flags(&std::env::var_os("CARGO_MAKEFLAGS")?)?.to_path_buf();
    let tokens = std::env::var(CPU_TOKENS_ENV).ok()?.parse().ok()?;
    Some(InheritedJobserver { fifo, tokens })
}

/// A job's jobserver: the fifo its command reads, and the count its command and test threads
/// are sized to.
#[derive(Debug)]
pub struct Jobserver {
    fifo: PathBuf,
    tokens: NonZeroUsize,
    /// The supervisor's own read-write descriptor of a fifo it made for this job, which keeps the
    /// token bytes alive; `None` for the requester's fifo this job reads too, which its own job
    /// owns.
    own: Option<File>,
}

impl Jobserver {
    pub fn fifo(&self) -> &Path {
        &self.fifo
    }

    pub fn tokens(&self) -> NonZeroUsize {
        self.tokens
    }

    /// A fifo of `tokens` in `directory`, for one job. Named by this supervisor's pid and a
    /// sequence, so no two supervisors -- not even two in one process -- name the same fifo.
    fn make(directory: &Path, tokens: NonZeroUsize) -> io::Result<Self> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        std::fs::create_dir_all(directory)?;
        let fifo = directory.join(format!(
            "{}-{}.fifo",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let path = CString::new(fifo.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "the jobserver fifo path has a NUL",
            )
        })?;
        // SAFETY: `path` is a NUL-terminated path that outlives the call.
        if unsafe { libc::mkfifo(path.as_ptr(), 0o600) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // Opened read-write, a fifo's open never waits for a peer. Unlinked by the drop on every
        // later failure.
        let mut jobserver = Self {
            own: None,
            tokens,
            fifo,
        };
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&jobserver.fifo)
            .inspect_err(|_| jobserver.unlink())?;
        file.write_all(&vec![TOKEN_BYTE; tokens.get() - 1])
            .inspect_err(|_| jobserver.unlink())?;
        jobserver.own = Some(file);
        Ok(jobserver)
    }

    fn unlink(&self) {
        if let Err(error) = std::fs::remove_file(&self.fifo)
            && error.kind() != io::ErrorKind::NotFound
        {
            cowshed_os::eprintln!(
                "cowshed: cannot unlink the jobserver fifo {}: {error}",
                self.fifo.display()
            );
        }
    }
}

impl Drop for Jobserver {
    fn drop(&mut self) {
        if self.own.take().is_some() {
            self.unlink();
        }
    }
}

/// Admit a job's command (05_gateway.md "Memory pressure", "Parallelism inside a job"): a start
/// whose class waits for memory waits while the host reports memory pressure, reading it again
/// every [`RECHECK_EVERY`], and says so on the job's stderr; then the job's jobserver is `parent`,
/// the requester's, when its fifo lies in `directory`, the job's own pool directory that its
/// sandbox opens, at the lesser of the two counts; or else a fifo of its own there holding
/// `tokens`. Dropping the future -- the job killed while it waits -- ends the wait.
pub(super) async fn admit(
    class: ExecClass,
    tokens: NonZeroUsize,
    directory: PathBuf,
    parent: Option<InheritedJobserver>,
) -> Result<Jobserver> {
    if crate::host_pressure::waits_for_memory(class) {
        wait_for_memory().await?;
    }
    match parent {
        Some(parent) if parent.fifo.parent() == Some(directory.as_path()) => Ok(Jobserver {
            fifo: parent.fifo,
            tokens: tokens.min(parent.tokens),
            own: None,
        }),
        Some(_) | None => Jobserver::make(&directory, tokens).map_err(|error| {
            CowshedError::cpu_admission(CpuAdmissionRefusal::Jobserver {
                directory,
                error: error.to_string(),
            })
        }),
    }
}

/// Wait until the host reports no memory pressure. The first pressured reading starts a wait
/// the job's stderr names, and the reading that ends it, or a failed one, closes it.
async fn wait_for_memory() -> Result<()> {
    let mut waiting: Option<std::time::Instant> = None;
    loop {
        let decision = read_memory_pressure().map(may_start).map_err(|error| {
            CowshedError::cpu_admission(CpuAdmissionRefusal::MemoryPressure {
                error: error.to_string(),
            })
        });
        match decision {
            Ok(Decision::Wait { pressure }) => {
                if waiting.is_none() {
                    progress(format!(
                        "cowshed: jobserver wait start queue=memory pressure={pressure}"
                    ))
                    .await;
                    waiting = Some(std::time::Instant::now());
                }
                tokio::time::sleep(RECHECK_EVERY).await;
            }
            Ok(Decision::Start) | Err(_) => {
                if let Some(started) = waiting {
                    let status = if decision.is_ok() { "ok" } else { "err" };
                    progress(format!(
                        "cowshed: jobserver wait done elapsed={:?} status={status}",
                        started.elapsed()
                    ))
                    .await;
                }
                return decision.map(|_| ());
            }
        }
    }
}

tokio::task_local! {
    /// A supervisor sends admission diagnostics down the job's existing stderr stream, not
    /// its own daemon log.
    static PROGRESS: (JobId, tokio::sync::mpsc::Sender<super::supervisor::ProcessEvent>);
}

pub(super) async fn reporting<T>(
    job: JobId,
    events: tokio::sync::mpsc::Sender<super::supervisor::ProcessEvent>,
    work: impl Future<Output = T>,
) -> T {
    PROGRESS.scope((job, events), work).await
}

async fn progress(line: String) {
    match PROGRESS.try_with(|(job, events)| (*job, events.clone())) {
        Ok((job_id, events)) => {
            // A closed receiver means the supervisor is gone; the admission future is then
            // cancelled with it.
            let _ = events
                .send(super::supervisor::ProcessEvent::Output {
                    job_id,
                    stream: crate::storage::job_artifact::StreamKind::Stderr,
                    bytes: bytes::Bytes::from(format!("{line}\n")),
                })
                .await;
        }
        Err(_) => cowshed_os::eprintln!("{line}"),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};

    use super::*;

    const FOUR: NonZeroUsize = NonZeroUsize::new(4).expect("four");

    /// A job's own fifo holds one token byte fewer than its count, since its command holds the
    /// implicit first one, is its user's alone, and is gone once the job lets it go. A requester's
    /// fifo in the job's own pool directory is the job's jobserver too, and outlives the job: it
    /// is its own job's to unlink.
    #[tokio::test]
    async fn a_job_serves_count_minus_one_bytes_and_shares_only_a_fifo_its_sandbox_opens() {
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-jobserver",
        );
        let directory = root.join("pool");
        let elsewhere = root.join("other-pool");

        let own = admit(ExecClass::Gate, FOUR, directory.clone(), None)
            .await
            .expect("a job's own fifo");
        let fifo = own.fifo().to_path_buf();
        assert_eq!(fifo.parent(), Some(directory.as_path()));
        let metadata = std::fs::metadata(&fifo).expect("the fifo exists");
        assert!(metadata.file_type().is_fifo());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let mut reader = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&fifo)
            .expect("open the fifo as Cargo does");
        let mut bytes = [0_u8; 8];
        assert_eq!(reader.read(&mut bytes).expect("read its tokens"), 3);

        let two = NonZeroUsize::new(2).expect("two");
        let parent = |fifo: PathBuf| Some(InheritedJobserver { fifo, tokens: two });
        let shared = admit(
            ExecClass::Gate,
            FOUR,
            directory.clone(),
            parent(fifo.clone()),
        )
        .await
        .expect("the requester's fifo");
        assert_eq!((shared.fifo(), shared.tokens()), (fifo.as_path(), two));
        let foreign = elsewhere.join("1-0.fifo");
        let separate = admit(ExecClass::Gate, FOUR, directory, parent(foreign.clone()))
            .await
            .expect("a fifo of its own");
        assert_eq!(separate.tokens(), FOUR);
        assert_ne!(separate.fifo(), foreign);

        drop(shared);
        assert!(fifo.exists(), "a shared fifo is its own job's to unlink");
        drop(own);
        assert!(
            !fifo.exists(),
            "a job's own fifo is unlinked when it lets it go"
        );
    }
}
