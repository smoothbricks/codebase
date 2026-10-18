//! The gateway's jobserver fifos (05_gateway.md, "Host CPU budget": one fifo per
//! client process).
//!
//! On a grant the gateway creates one fifo here, keeps its own `O_RDWR` descriptor
//! until the drain so the token bytes survive while no client has it open, and
//! writes `g − 1` token bytes into it: the client's implicit make token is the
//! first. When the client's connection closes the gateway drains the bytes left,
//! unlinks the fifo and the caller returns all `g` tokens to the ledger, so
//! tokens a killed process read and never wrote back are credited too. A restart
//! forgets every grant, so startup removes every fifo left here.

use std::fs::{File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::FileTypeExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The byte a GNU make jobserver hands out as one token.
const TOKEN_BYTE: u8 = b'+';

/// Whole-grant admission, never a request to resize a live FIFO. Neither class has an admission
/// deadline: a request waits in the ledger's order until it is granted or its client leaves.
#[derive(Clone, Copy, Debug)]
pub(crate) enum JobserverAdmission {
    Background,
    Gate,
}

impl JobserverAdmission {
    /// The root request's rank: a gate admission is a gate job of its own, its ticket its age.
    pub(crate) fn rank(self) -> crate::cpu_budget::Rank {
        match self {
            Self::Background => crate::cpu_budget::Rank::Background,
            Self::Gate => crate::cpu_budget::Rank::Gate,
        }
    }
}

/// The fifos of one gateway, in its private runtime directory.
#[derive(Clone, Debug)]
pub(crate) struct JobserverFifos {
    directory: std::sync::Arc<PathBuf>,
    counter: std::sync::Arc<AtomicU64>,
}

impl JobserverFifos {
    /// The trusted job identity chooses the workspace IPC scope; clients cannot name a path.
    pub(crate) fn create_pool(&self, source: PoolSource, incarnation: &str) -> io::Result<CpuPool> {
        let store = self.directory.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "jobserver directory has no runtime parent",
            )
        })?;
        let directory = cowshed_gateway_types::cpu_pool_directory(store, incarnation)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        CpuPool::start(std::sync::Arc::new(directory), source)
    }

    /// Name the fifo directory, sweeping a previous gateway's leftover fifos.
    pub(crate) fn start(directory: PathBuf) -> Self {
        // Sweep the fifos a previous gateway left in the directory when it
        // exists: a restart forgot those grants, and their bytes belong to
        // nobody. The directory itself is created at the first grant, so
        // gateway start neither creates the control socket's parent (masking
        // its absence) nor fails on it.
        if let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_fifo()) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        Self {
            directory: std::sync::Arc::new(directory),
            counter: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create one fifo holding `tokens - 1` token bytes, kept open by the
    /// gateway's own descriptor until [`JobserverFifo::drain_and_unlink`].
    pub(crate) fn create(&self, tokens: usize) -> io::Result<JobserverFifo> {
        std::fs::create_dir_all(self.directory.as_ref() as &Path)?;
        let sequence = self.counter.fetch_add(1, Ordering::Relaxed);
        let path = self
            .directory
            .join(format!("jobserver-{}-{sequence}.fifo", std::process::id()));
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "jobserver fifo path has a NUL")
        })?;
        // SAFETY: c_path is a valid NUL-terminated path for the duration of the call.
        if unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let opened = OpenOptions::new().read(true).write(true).open(&path);
        let mut file = match opened {
            Ok(file) => file,
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                return Err(error);
            }
        };
        if tokens > 1
            && let Err(error) = file.write_all(&vec![TOKEN_BYTE; tokens - 1])
        {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        Ok(JobserverFifo { path, file })
    }
}

/// One granted client's fifo, held open by the gateway until the drain.
#[derive(Debug)]
pub(crate) struct JobserverFifo {
    path: PathBuf,
    file: File,
}

impl JobserverFifo {
    /// The path the granted answer names for `CARGO_MAKEFLAGS`.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Hand `tokens` more token bytes to the running client: what a growing grant was given.
    pub(crate) fn fill(&mut self, tokens: usize) -> io::Result<()> {
        self.file.write_all(&vec![TOKEN_BYTE; tokens])
    }

    /// Drain the token bytes left in the fifo and unlink it. A cargo that
    /// outlives its launcher keeps its own descriptor and runs over budget
    /// until it finishes; what it still holds is not the gateway's to reclaim.
    pub(crate) fn drain_and_unlink(mut self) {
        // SAFETY: the descriptor is this fifo's own, opened above.
        let flags = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_GETFL) };
        if flags >= 0 {
            // SAFETY: same descriptor; read what is already there, never wait.
            unsafe {
                libc::fcntl(
                    self.file.as_raw_fd(),
                    libc::F_SETFL,
                    flags | libc::O_NONBLOCK,
                );
            }
            let mut discard = [0_u8; 4096];
            while self.file.read(&mut discard).is_ok_and(|read| read > 0) {}
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

use std::os::unix::io::AsRawFd as _;

/// Where a pool's children take their tokens.
pub(crate) enum PoolSource {
    /// A background grant's own allocation. Its actor uses the same ledger as host admission,
    /// with a single checkout, so sibling grants keep the parent pool's FIFO/min(want,free) policy
    /// and never sum past it.
    Lent(std::num::NonZeroUsize),
    /// A gate grant: each child asks the host budget itself, at gate class, for the gate's
    /// checkout, ranked by the gate job's own admission. A land's tasks therefore rank ahead of
    /// background work at every token any holder returns, and an older land's tasks ahead of a
    /// younger one's; no allocation is held idle between tasks and no running grant needs to
    /// shrink.
    Relay {
        host: crate::cpu_budget::CpuBudget,
        checkout: std::sync::Arc<str>,
        job: crate::cpu_budget::JobAge,
    },
}

/// One pool endpoint serving a granted job's children.
pub(crate) struct CpuPool {
    path: PathBuf,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl CpuPool {
    fn start(directory: std::sync::Arc<PathBuf>, source: PoolSource) -> io::Result<Self> {
        use base64::Engine as _;
        use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory.as_path())?;
        // Flat, opaque endpoints stay within Darwin's sun_path bound and cannot be guessed
        // from the job's public id. Sandbox policy admits connect/metadata, never directory data.
        let name = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(uuid::Uuid::new_v4().as_bytes());
        let path = directory.join(name);
        let listener =
            cowshed_os::fork_lock::Fenced::create(|| tokio::net::UnixListener::bind(&path))?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let (budget, checkout, rank) = match source {
            PoolSource::Lent(tokens) => (
                crate::cpu_budget::CpuBudget::start(crate::cpu_budget::CpuBudgetLimits { tokens }),
                std::sync::Arc::from("parent-pool"),
                crate::cpu_budget::Rank::Background,
            ),
            PoolSource::Relay {
                host,
                checkout,
                job,
            } => (host, checkout, crate::cpu_budget::Rank::GateTask(job)),
        };
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = async {
                        use std::future::Future as _;
                        let mut accepting = std::pin::pin!(listener.accept());
                        std::future::poll_fn(|cx| cowshed_os::fork_lock::create(|| Ok(accepting.as_mut().poll(cx)))
                            .expect("polling socket accept runs an infallible closure")).await
                    } => match accepted {
                        Ok((stream, _)) => {
                            children.spawn(serve_pool(cowshed_os::fork_lock::Fenced::new(stream), budget.clone(), std::sync::Arc::clone(&checkout), rank, std::sync::Arc::clone(&directory)));
                        }
                        Err(error) => { eprintln!("cowshed: CPU pool listener failed: {error}"); break; }
                    },
                    ended = children.join_next(), if !children.is_empty() => {
                        if let Some(Err(error)) = ended { eprintln!("cowshed: CPU pool child failed: {error}"); }
                    }
                }
            }
            children.shutdown().await;
        });
        Ok(Self {
            path,
            task: Some(task),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) async fn close(mut self) -> io::Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                return Err(io::Error::other(format!("CPU pool task failed: {error}")));
            }
        }
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl Drop for CpuPool {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!(
                "cowshed: cannot remove CPU pool {}: {error}",
                self.path.display()
            );
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
enum PoolRequest {
    #[serde(rename = "cpu-pool")]
    Take {
        want: std::num::NonZeroUsize,
        #[serde(default)]
        children: bool,
        /// The child is a Cargo build whose jobserver can hand out tokens it is given after it
        /// starts: it is told each new total, up to `want`.
        #[serde(default)]
        grow: bool,
    },
}

#[derive(serde::Serialize)]
#[serde(tag = "lease", rename_all = "kebab-case")]
enum PoolGrant<'a> {
    Queued {
        ok: bool,
    },
    Granted {
        ok: bool,
        tokens: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        pool: Option<&'a Path>,
    },
    /// A growing child now holds `tokens` in all.
    Grown {
        ok: bool,
        tokens: usize,
    },
}

async fn pool_reply(
    writer: &mut cowshed_os::fork_lock::Fenced<tokio::net::UnixStream>,
    answer: &impl serde::Serialize,
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let mut frame = serde_json::to_vec(answer).map_err(io::Error::other)?;
    frame.push(b'\n');
    writer.write_all(&frame).await
}

async fn serve_pool(
    stream: cowshed_os::fork_lock::Fenced<tokio::net::UnixStream>,
    budget: crate::cpu_budget::CpuBudget,
    checkout: std::sync::Arc<str>,
    rank: crate::cpu_budget::Rank,
    directory: std::sync::Arc<PathBuf>,
) {
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader};
    let mut reader = BufReader::new(stream);
    let mut frame = Vec::new();
    let parsed = match (&mut reader).take(4097).read_until(b'\n', &mut frame).await {
        Ok(_) if frame.len() <= 4096 && frame.ends_with(b"\n") => {
            serde_json::from_slice::<PoolRequest>(&frame)
        }
        Ok(_) => {
            let _ = pool_reply(reader.get_mut(), &serde_json::json!({"ok":false,"code":"invalid-request","error":"a CPU pool request is one line of at most 4096 bytes"})).await;
            return;
        }
        Err(error) => {
            eprintln!("cowshed: cannot read CPU pool request: {error}");
            return;
        }
    };
    let PoolRequest::Take {
        want,
        children,
        grow,
    } = match parsed {
        Ok(request) => request,
        Err(error) => {
            let _ = pool_reply(
                reader.get_mut(),
                &serde_json::json!({"ok":false,"code":"invalid-request","error":error.to_string()}),
            )
            .await;
            return;
        }
    };
    let claim = crate::disk_lease::Claim::new("child CPU pool", None);
    let (stake, granted, mut growth) = if grow {
        let (stake, granted, growth) = budget.ask_growing(checkout, want, claim, rank);
        (stake, granted, Some(growth))
    } else {
        let (stake, granted) = budget.ask(checkout, want, claim, rank);
        (stake, granted, None)
    };
    if pool_reply(reader.get_mut(), &PoolGrant::Queued { ok: true })
        .await
        .is_err()
    {
        return;
    }
    let mut byte = [0_u8; 1];
    let tokens = tokio::select! {
        granted = granted => match granted {
            Ok(tokens) => tokens,
            Err(_) => {
                let _ = pool_reply(reader.get_mut(), &serde_json::json!({"ok":false,"code":"rejected","error":"the parent CPU pool stopped"})).await;
                return;
            }
        },
        _ = reader.read(&mut byte) => return,
    };
    let pool = if children {
        let tokens = std::num::NonZeroUsize::new(tokens).expect("a CPU grant is nonzero");
        match CpuPool::start(directory, PoolSource::Lent(tokens)) {
            Ok(pool) => Some(pool),
            Err(error) => {
                let _ = pool_reply(reader.get_mut(), &serde_json::json!({"ok":false,"code":"rejected","error":format!("cannot serve child CPU pool: {error}")})).await;
                return;
            }
        }
    } else {
        None
    };
    if pool_reply(
        reader.get_mut(),
        &PoolGrant::Granted {
            ok: true,
            tokens,
            pool: pool.as_ref().map(CpuPool::path),
        },
    )
    .await
    .is_err()
    {
        return;
    }
    // End of stream or post-request bytes end the local allocation; a child never releases the
    // parent's upstream grant and never takes a second host allocation while it is borrowed.
    // Until then a growing child hears each new total its allocation reaches.
    loop {
        let grown = async {
            match growth.as_mut() {
                Some(growth) => growth.recv().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = reader.read(&mut byte) => break,
            grown = grown => match grown {
                Some(tokens) => {
                    if pool_reply(reader.get_mut(), &PoolGrant::Grown { ok: true, tokens }).await.is_err() {
                        break;
                    }
                }
                // The budget is gone: nothing more will grow, and the allocation stands.
                None => growth = None,
            },
        }
    }
    if let Some(pool) = pool
        && let Err(error) = pool.close().await
    {
        eprintln!("cowshed: child CPU pool did not close: {error}");
    }
    drop(stake);
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;
    fn directory() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("cowshed-jobserver-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let directory = root.join("jobserver");
        (root, directory)
    }

    #[test]
    fn a_grant_fifo_holds_one_fewer_token_than_the_grant_and_drains_clean() {
        let (_root, directory) = directory();
        let fifos = JobserverFifos::start(directory.clone());
        let fifo = fifos.create(4).unwrap();
        let path = fifo.path().to_path_buf();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // A client takes two of the three token bytes, then dies without
        // writing them back: the drain removes what is left and the path.
        let mut client = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut taken = [0_u8; 2];
        client.read_exact(&mut taken).unwrap();
        assert_eq!(taken, [TOKEN_BYTE; 2]);
        drop(client);
        fifo.drain_and_unlink();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn workspace_pool_binds_and_serves_with_a_real_fixture_length_store() {
        use std::os::unix::fs::DirBuilderExt as _;
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

        let root = std::fs::canonicalize("/tmp").unwrap().join(format!(
            "cowshed-dtest-{}-{}-cli",
            std::process::id(),
            &uuid::Uuid::new_v4().simple().to_string()[..4]
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let store = root.join("store");
        let incarnation = "0123456789abcdef0123456789abcdef";
        let previous_layout = store
            .join("run/cpu-pool")
            .join(incarnation)
            .join("abcdefghijklmnopqrstuv");
        assert!(previous_layout.as_os_str().as_encoded_bytes().len() >= 104);
        let fifos = JobserverFifos::start(store.join("jobserver"));
        let pool = fifos
            .create_pool(
                PoolSource::Lent(std::num::NonZeroUsize::new(2).unwrap()),
                incarnation,
            )
            .expect("the actual private pool binds");
        let endpoint = pool.path().to_owned();
        assert_eq!(
            endpoint.parent(),
            Some(
                cowshed_gateway_types::cpu_pool_directory(&store, incarnation)
                    .unwrap()
                    .as_path()
            )
        );
        assert!(endpoint.as_os_str().as_encoded_bytes().len() < 104);
        assert_eq!(
            std::fs::metadata(&endpoint).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let stream = tokio::net::UnixStream::connect(&endpoint).await.unwrap();
        let mut reader = BufReader::new(stream);
        reader
            .get_mut()
            .write_all(b"{\"op\":\"cpu-pool\",\"want\":2}\n")
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let queued: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(queued["lease"], "queued");
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let granted: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(granted["lease"], "granted");
        assert_eq!(granted["tokens"], 2);
        drop(reader);
        pool.close().await.expect("the actual pool retires");
        assert!(!endpoint.exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn startup_removes_leftover_fifos_and_nothing_else() {
        let (_root, directory) = directory();
        std::fs::create_dir_all(&directory).unwrap();
        let leftover = directory.join("jobserver-1-0.fifo");
        let c_path = std::ffi::CString::new(leftover.as_os_str().as_bytes()).unwrap();
        // SAFETY: c_path is a valid NUL-terminated path for the call.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let other = directory.join("control.sock-note");
        std::fs::write(&other, b"keep").unwrap();
        let _fifos = JobserverFifos::start(directory);
        assert!(!leftover.exists());
        assert!(other.exists());
    }
}
