//! A job's jobserver (05_gateway.md "Host scheduling", "Parallelism inside a job"). The kernel
//! schedules CPU among jobs; a jobserver bounds only how many compiler and test threads one
//! build tree starts, so a `cargo` or `nextest` does not hand the kernel ten times the host's
//! cores. Every job's command reads a GNU make jobserver fifo its supervisor serves with a
//! constant count: the host's cores, or the job's smaller want. Nothing here waits: whether and
//! when a job starts is its admission's ([`super::job_admission`]).
//!
//! The fifo is the job's own, in the workspace's pool directory ([`cpu_pool_directory`]), which
//! the job's sandbox opens for FIFOs alone (04_sandbox.md). The supervisor makes it mode `0600`,
//! keeps its own read-write descriptor, so the token bytes survive while no reader has it open,
//! writes `tokens − 1` bytes (the command holds the implicit first token, as make's protocol has
//! it), and unlinks it when the job concludes. A Cargo that outlives the job keeps its open
//! descriptor; one started after finds no fifo, runs unconfigured, and says so itself.

use std::collections::BTreeMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::unix::AsyncFd;
use tokio::sync::oneshot;

use crate::error::{CowshedError, CpuAdmissionRefusal, Result};
use crate::metadata::WorkspaceIncarnation;

/// The count a job's command reads beside its fifo (05_gateway.md "Cargo reads its holder's
/// jobserver").
pub const CPU_TOKENS_ENV: &str = "BOUNDED_EXEC_CPU_TOKENS";

/// The make flags a jobserver exports to Cargo and make, ahead of its fifo's path.
pub(crate) const JOBSERVER_FIFO_FLAGS: &str = "-j --jobserver-auth=fifo:";

/// One token: any byte is one to make and Cargo.
const TOKEN_BYTE: u8 = b'+';

/// The workspace's pool directory, `<store>/run/cpu/<incarnation as base64url>`: the one
/// directory its sandbox opens for FIFOs, where its jobs' jobserver fifos live. Sibling
/// workspaces' directories sit beside it, and no sandbox can list `run/cpu`.
pub fn cpu_pool_directory(store: &Path, incarnation: &WorkspaceIncarnation) -> PathBuf {
    use base64::Engine as _;
    // A validated incarnation is 32 lowercase hex digits: 16 bytes.
    let digit = |byte: u8| {
        if byte <= b'9' {
            byte - b'0'
        } else {
            byte - b'a' + 10
        }
    };
    let mut identity = [0_u8; 16];
    for (slot, pair) in identity
        .iter_mut()
        .zip(incarnation.as_str().as_bytes().as_chunks::<2>().0)
    {
        *slot = digit(pair[0]) * 16 + digit(pair[1]);
    }
    let mut encoded = [0_u8; 22];
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(identity, &mut encoded)
        .expect("22 bytes hold a complete base64url 128-bit incarnation");
    let name = std::str::from_utf8(&encoded).expect("base64url is ASCII");
    store.join("run/cpu").join(name)
}

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
    /// Whether the requester runs under a hold on mbx's pool (`MBX_SCHED_DISABLE=1`, what a held
    /// job and `mbx reserve`'s own command see): the new job's work is covered by that hold, so it
    /// takes no place in the pool of its own (05_gateway.md "Held jobs").
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub held: bool,
}

/// The jobserver this process inherited from the job that started it, if any: the fifo its
/// `CARGO_MAKEFLAGS` names beside the count its [`CPU_TOKENS_ENV`] names, and whether that job runs
/// under a hold on mbx's pool. The controller runs in the requester's process (in-process for the
/// CLI and every embedder; `cowshed controller` inherits its embedder's environment), so a
/// `cowshed exec` from inside a job names its parent's jobserver, which the new job reads too when
/// its sandbox can open it. A fifo beside no count, or a count that is no positive count, names
/// nothing.
pub fn requester_jobserver() -> Option<InheritedJobserver> {
    let fifo = fifo_in_flags(&std::env::var_os("CARGO_MAKEFLAGS")?)?.to_path_buf();
    let tokens = std::env::var(CPU_TOKENS_ENV).ok()?.parse().ok()?;
    let held = std::env::var_os("MBX_SCHED_DISABLE").is_some_and(|value| value == "1");
    Some(InheritedJobserver { fifo, tokens, held })
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

    /// Duplicate the supervisor's own read-write fifo descriptor as a non-blocking [`File`]
    /// (`O_NONBLOCK` + `FD_CLOEXEC`) for event-driven draining during a land's gate span. Returns
    /// `Ok(None)` when this job shares a parent's fifo (withdrawn from with its parent) or holds
    /// only the implicit token (`tokens == 1`).
    pub(crate) fn dup_nonblocking_fd(&self) -> io::Result<Option<File>> {
        let Some(file) = &self.own else {
            return Ok(None);
        };
        if self.tokens.get() <= 1 {
            return Ok(None);
        }
        // SAFETY: `file` is an open descriptor owned by `self`.
        let dup = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if dup < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `dup` is a freshly duplicated owned descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(dup) };
        // SAFETY: reading and setting `O_NONBLOCK` on our owned duplicated descriptor.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: setting `O_NONBLOCK` on our owned duplicated descriptor.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Some(File::from(fd)))
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

/// Event-driven drainer holding withdrawn token bytes for one job's fifo during a land's gate
/// span (05_gateway.md "A land's gate takes tokens back"). Dropping it without calling
/// [`Self::restore`] cancels the drain without writing bytes back (for a job that ended).
#[derive(Debug)]
pub(crate) struct FifoDrainer {
    stop: Option<oneshot::Sender<bool>>,
    task: Option<tokio::task::JoinHandle<usize>>,
}

impl FifoDrainer {
    /// Start draining `file` (a non-blocking duplicated read-write descriptor from
    /// [`Jobserver::dup_nonblocking_fd`]), sending each withdrawn byte count through `tx`.
    ///
    /// Watches `span` directly so when the gate span goes off (or is already off at start), the
    /// drainer stops draining and writes every held byte back without waiting for an actor turn.
    /// [`Self::restore`] joins the task to collect the restored byte count for span recording.
    pub(crate) fn start<T: Send + 'static>(
        file: File,
        mut span: tokio::sync::watch::Receiver<bool>,
        tx: tokio::sync::mpsc::Sender<T>,
        map: impl Fn(usize) -> T + Send + 'static,
    ) -> io::Result<Self> {
        let async_fd = AsyncFd::new(file)?;
        let (stop_tx, mut stop_rx) = oneshot::channel::<bool>();
        let task = tokio::spawn(async move {
            let mut held = Vec::<u8>::new();
            let mut buf = [0_u8; 256];
            if !*span.borrow() {
                return 0;
            }
            let initial = drain_file(async_fd.get_ref(), &mut buf, &mut held);
            let mut restore_override = None;
            if initial > 0 {
                tokio::select! {
                    biased;
                    stop = &mut stop_rx => {
                        restore_override = Some(stop.unwrap_or(false));
                    }
                    () = wait_span_off(&mut span) => {
                        restore_override = Some(true);
                    }
                    _ = tx.send(map(initial)) => {}
                }
            }
            let restore = match restore_override {
                Some(restore) => restore,
                None => loop {
                    tokio::select! {
                        biased;
                        stop = &mut stop_rx => break stop.unwrap_or(false),
                        () = wait_span_off(&mut span) => break true,
                        ready = async_fd.readable() => {
                            let Ok(mut guard) = ready else {
                                break false;
                            };
                            let count = drain_file(async_fd.get_ref(), &mut buf, &mut held);
                            if count > 0 {
                                drop(guard);
                                let interrupted = tokio::select! {
                                    biased;
                                    stop = &mut stop_rx => Some(stop.unwrap_or(false)),
                                    () = wait_span_off(&mut span) => Some(true),
                                    _ = tx.send(map(count)) => None,
                                };
                                if let Some(restore) = interrupted {
                                    break restore;
                                }
                            } else {
                                guard.clear_ready();
                            }
                        }
                    }
                },
            };
            if restore && !held.is_empty() {
                let mut written = 0;
                while written < held.len() {
                    match write_file(async_fd.get_ref(), &held[written..]) {
                        Ok(0) => break,
                        Ok(n) => written += n,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            let Ok(mut guard) = async_fd.writable().await else {
                                break;
                            };
                            guard.clear_ready();
                        }
                        Err(_) => break,
                    }
                }
                return written;
            }
            0
        });
        Ok(Self {
            stop: Some(stop_tx),
            task: Some(task),
        })
    }

    /// Stop draining, write every withdrawn token byte back to the fifo, and return how many bytes
    /// were restored.
    pub(crate) async fn restore(mut self) -> usize {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
        }
        match self.task.take() {
            Some(task) => task.await.unwrap_or(0),
            None => 0,
        }
    }
}

impl Drop for FifoDrainer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(false);
        }
    }
}

/// Resolve once the gate span is off, or its publisher is gone, which is the same: a supervisor
/// that no longer hears the gate state takes it to be off (05_gateway.md "A land's gate takes
/// tokens back").
pub(crate) async fn wait_span_off(span: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = span.wait_for(|&active| !active).await;
}

fn drain_file(mut file: &File, buf: &mut [u8], held: &mut Vec<u8>) -> usize {
    use std::io::Read as _;
    let mut total = 0;
    loop {
        match file.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                held.extend_from_slice(&buf[..n]);
                total += n;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    total
}

fn write_file(mut file: &File, bytes: &[u8]) -> io::Result<usize> {
    loop {
        match file.write(bytes) {
            Ok(n) => return Ok(n),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
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

/// The jobserver of an admitted job (05_gateway.md "Parallelism inside a job"): `parent`, the
/// requester's, when its fifo lies in `directory`, the job's own pool directory that its sandbox
/// opens, at the lesser of the two counts; or else a fifo of its own there. A fifo of its own holds
/// `tokens`, or, when the requester runs under a hold on mbx's pool, no more than that hold's
/// count: the job's work is the hold's, as mbx's own enclosed reservation's is (05_gateway.md
/// "Held jobs").
pub(super) fn jobserver(
    tokens: NonZeroUsize,
    directory: PathBuf,
    parent: Option<InheritedJobserver>,
) -> Result<Jobserver> {
    match parent {
        Some(parent) if parent.fifo.parent() == Some(directory.as_path()) => Ok(Jobserver {
            fifo: parent.fifo,
            tokens: tokens.min(parent.tokens),
            own: None,
        }),
        parent => {
            let tokens = match parent {
                Some(parent) if parent.held => tokens.min(parent.tokens),
                Some(_) | None => tokens,
            };
            Jobserver::make(&directory, tokens).map_err(|error| {
                CowshedError::cpu_admission(CpuAdmissionRefusal::Jobserver {
                    directory,
                    error: error.to_string(),
                })
            })
        }
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
    #[test]
    fn a_job_serves_count_minus_one_bytes_and_shares_only_a_fifo_its_sandbox_opens() {
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-jobserver",
        );
        let directory = root.join("pool");
        let elsewhere = root.join("other-pool");

        let own = jobserver(FOUR, directory.clone(), None).expect("a job's own fifo");
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
        let parent = |fifo: PathBuf| {
            Some(InheritedJobserver {
                fifo,
                tokens: two,
                held: false,
            })
        };
        let shared =
            jobserver(FOUR, directory.clone(), parent(fifo.clone())).expect("the requester's fifo");
        assert_eq!((shared.fifo(), shared.tokens()), (fifo.as_path(), two));
        let foreign = elsewhere.join("1-0.fifo");
        let separate =
            jobserver(FOUR, directory, parent(foreign.clone())).expect("a fifo of its own");
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

    /// A requester under a hold on mbx's pool, its fifo in a pool this job's sandbox cannot open,
    /// still bounds the job: a fifo of its own holds no more than the hold's count, and one whose
    /// requester holds nothing holds the job's own count.
    #[test]
    fn a_fifo_of_its_own_holds_no_more_than_its_requesters_hold() {
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cowshed-jobserver-held",
        );
        let two = NonZeroUsize::new(2).expect("two");
        let requester = |held: bool| {
            Some(InheritedJobserver {
                fifo: root.join("other-pool").join("1-0.fifo"),
                tokens: two,
                held,
            })
        };
        let enclosed = jobserver(FOUR, root.join("pool"), requester(true)).expect("a fifo");
        assert_eq!(enclosed.tokens(), two);
        assert_ne!(enclosed.fifo(), root.join("other-pool").join("1-0.fifo"));
        let unheld = jobserver(FOUR, root.join("pool"), requester(false)).expect("a fifo");
        assert_eq!(unheld.tokens(), FOUR);
    }
}
