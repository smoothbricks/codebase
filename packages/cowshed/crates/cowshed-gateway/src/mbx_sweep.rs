//! The host's mbx store sweep (05_gateway.md, "The mbx store sweep").
//!
//! mbx's automatic sweep collects the one store every job's builds share, so it is a host-shared
//! process, never a job member (11_shell.md, "Shell activation and process reuse"): no build in a
//! job starts it (`MBX_GC_AUTO=0`, 11_shell.md "Canonical mbx environment"), and this timer, a
//! thread of the gateway, which no job contains, runs the installed mbx's `gc --automatic` at its
//! start and then [`MBX_SWEEP_EVERY`] after each run ends. mbx decides whether a sweep is due and
//! holds its collector lock while it sweeps; the timer waits for each run, so it never starts one
//! beside another. Each run leads a process group of its own, which neither a job's retirement nor
//! launchd's teardown of the gateway's group reaches.

use std::fmt;
use std::fs::File;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use cowshed_core::capabilities::mbx::{HostMbx, observe_host};
use cowshed_os::fork_lock::Spawn as _;

/// The wait between one run's end and the next: mbx's shortest interval between sweeps, the five
/// minutes it keeps while a disk is short of `gc.min_free_size` (mbx's `LOW_DISK_INTERVAL`), so a
/// sweep that comes due starts at most this late. A run that finds none due costs a spawn, the
/// collector lock, one `stat` of mbx's stamp and at most four `statfs`.
pub const MBX_SWEEP_EVERY: Duration = Duration::from_secs(5 * 60);

/// mbx's sweep log under its store (`MBX_CACHE_DIR`), where mbx's own collector writes its stderr.
const SWEEP_LOG: &str = "actions/gc/v1/sweep.log";

/// The sweep the gateway runs.
#[derive(Clone, Debug)]
pub struct MbxSweep {
    /// The home whose installed mbx sweeps (`dev.cowshed/bin/mbx`, 14_nix.md "mbx") and whose
    /// mbx settings it reads.
    pub home: PathBuf,
    /// The store every job's canonical environment names as `MBX_CACHE_DIR`.
    pub store: PathBuf,
    /// The wait between one run's end and the next.
    pub every: Duration,
}

/// The running timer. Dropping it ends the timer once the run under way, if any, has ended; the
/// run itself is never signalled.
#[derive(Debug)]
pub struct MbxSweepTimer {
    _stop: mpsc::Sender<()>,
}

/// Why a pass started no sweep.
#[derive(Debug, PartialEq, Eq)]
enum Unswept {
    NoMbx,
    Unresolved { link: PathBuf, cause: String },
    Unstarted { program: PathBuf, cause: String },
}

impl fmt::Display for Unswept {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoMbx => write!(formatter, "no mbx is installed beside cowshed"),
            Self::Unresolved { link, cause } => write!(
                formatter,
                "the installed mbx {} does not resolve ({cause}); next: cowshed setup",
                link.display()
            ),
            Self::Unstarted { program, cause } => write!(
                formatter,
                "{} gc --automatic could not start: {cause}",
                program.display()
            ),
        }
    }
}

impl MbxSweep {
    /// Start the timer on a thread of its own, its first run at once. mbx's sweep log is opened
    /// here, once, and every run's stderr is appended to it.
    pub fn start(self) -> std::io::Result<MbxSweepTimer> {
        let log = sweep_log(&self.store);
        let (stop, stopped) = mpsc::channel();
        std::thread::Builder::new()
            .name("mbx-sweep".to_owned())
            .spawn(move || self.run(log.as_ref(), &stopped))?;
        Ok(MbxSweepTimer { _stop: stop })
    }

    fn run(&self, log: Option<&File>, stopped: &mpsc::Receiver<()>) {
        let mut said: Option<Unswept> = None;
        loop {
            match self.pass(log) {
                Ok(()) => said = None,
                Err(unswept) => {
                    // Said once, and again only when the reason changes: a host without mbx
                    // would otherwise say so every five minutes.
                    if said.as_ref() != Some(&unswept) {
                        cowshed_os::eprintln!("cowshed: mbx sweep: {unswept}");
                    }
                    said = Some(unswept);
                }
            }
            // The event is the interval's passing: whether a sweep is due is mbx's to decide
            // against its own stamp, so no event of the gateway's says so. Nothing is ever sent;
            // the handle's drop ends the timer.
            match stopped.recv_timeout(self.every) {
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// One run of the installed mbx's `gc --automatic`, waited for to its end.
    fn pass(&self, log: Option<&File>) -> Result<(), Unswept> {
        let program = match observe_host(&self.home) {
            HostMbx::Installed(program) => program,
            HostMbx::Absent => return Err(Unswept::NoMbx),
            HostMbx::Unresolved { link, cause } => {
                return Err(Unswept::Unresolved { link, cause });
            }
        };
        let stderr = match log.map(File::try_clone) {
            Some(Ok(log)) => Stdio::from(log),
            Some(Err(error)) => {
                cowshed_os::eprintln!(
                    "cowshed: mbx sweep's stderr is discarded this run: its log could not be shared: {error}"
                );
                Stdio::null()
            }
            None => Stdio::null(),
        };
        let mut command = Command::new(&program);
        command
            .args(["gc", "--automatic"])
            .current_dir(&self.store)
            // The host's own mbx settings decide, never a value the gateway was started with.
            .env_clear()
            .env("HOME", &self.home)
            .env("MBX_CACHE_DIR", &self.store)
            .env("MBX_TARGET_VIEWS", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .process_group(0);
        let mut child = command.spawn_locked().map_err(|error| Unswept::Unstarted {
            program: program.clone(),
            cause: error.to_string(),
        })?;
        let pid = child.id();
        cowshed_os::eprintln!(
            "cowshed: mbx sweep pid {pid} started: {} gc --automatic, MBX_CACHE_DIR={}",
            program.display(),
            self.store.display()
        );
        let began = Instant::now();
        // WHY unbounded: the sweep's own exit ends this wait, which nothing else waits on; a sweep
        // started beside it would find mbx's collector lock held and exit at once.
        #[allow(clippy::disallowed_methods)]
        let waited = child.wait();
        match waited {
            Ok(status) => cowshed_os::eprintln!(
                "cowshed: mbx sweep pid {pid} exited {status} after {:?}",
                began.elapsed()
            ),
            Err(error) => cowshed_os::eprintln!(
                "cowshed: mbx sweep pid {pid}: its exit could not be read after {:?}: {error}",
                began.elapsed()
            ),
        }
        Ok(())
    }
}

/// mbx's sweep log, opened to append as mbx's own collector opens it. A log that cannot be opened
/// leaves every run's stderr discarded, said on the gateway's: a full disk is when the sweep
/// matters most, so the runs go on without it.
fn sweep_log(store: &Path) -> Option<File> {
    let log = store.join(SWEEP_LOG);
    let opened = std::fs::create_dir_all(log.parent().expect("the sweep log has a parent"))
        .and_then(|()| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
        });
    match opened {
        Ok(file) => Some(file),
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: mbx sweep's stderr is discarded: {}: {error}",
                log.display()
            );
            None
        }
    }
}
