//! Scratch roots for tests that drive the host's real APFS stack.
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Every scratch root lives directly under this prefix and spells out the pid of the run that
/// owns it. The pid is the whole cleanup protocol: a later run can tell a live root from an
/// abandoned one without any lock file or shared state of its own.
const ROOT_PREFIX: &str = "/private/tmp/cowshed-itest-";

/// One test's disposable root: `/private/tmp/cowshed-itest-<pid>-<n>-<label>`.
///
/// Dropping it detaches every image attached from below it, then removes the tree. A run that
/// could not drop it (a SIGKILL, a harness timeout) leaves it to the next run's sweep.
pub struct ScratchRoot {
    path: PathBuf,
}

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        static SWEEP: Once = Once::new();
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        SWEEP.call_once(sweep_dead_runs);
        let path = PathBuf::from(format!(
            "{ROOT_PREFIX}{}-{}-{label}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        // A failed test unwinds with its volumes still attached; removing the tree without
        // detaching first strands kernel attachments pointing into a half-deleted root, and
        // leaves their volumes showing up in Finder and `mount` until the machine reboots.
        let root = self.path.to_string_lossy().into_owned();
        detach_images(|image| image.starts_with(&root));
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "scratch root {} was not removed ({error}); the next run's sweep reclaims it",
                self.path.display()
            );
        }
    }
}

/// Reclaim what runs that can no longer clean up after themselves left behind. Nothing runs after
/// a SIGKILL — a harness timeout, a bounded-exec force-kill, a `cargo test` killed mid-mount — so
/// the only protocol that always converges is that every run sweeps its dead predecessors.
///
/// The sweep is driven off the attachment table rather than the directory listing, because the
/// two residues outlive each other independently: a root directory can be deleted (by hand, or by
/// a tmp reaper) while its images stay attached, and an attached image keeps working from a
/// deleted backing file. Detaching therefore selects on the image path `hdiutil` still reports,
/// not on what is on disk now.
fn sweep_dead_runs() {
    detach_images(|image| owner_pid(image).is_some_and(process_is_gone));
    let Ok(entries) = fs::read_dir("/private/tmp") else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if owner_pid(&path.to_string_lossy()).is_some_and(process_is_gone)
            && let Err(error) = fs::remove_dir_all(&path)
        {
            eprintln!(
                "abandoned scratch root {} was not removed: {error}",
                path.display()
            );
        }
    }
}

/// The pid of the run that owns a scratch root, given the root itself or any path below it.
fn owner_pid(path: &str) -> Option<i32> {
    path.strip_prefix(ROOT_PREFIX)?
        .split(['-', '/'])
        .next()?
        .parse()
        .ok()
}

/// Whether no process holds `pid` any more. Signal 0 runs the existence and permission checks
/// without delivering anything, and `ESRCH` is the single answer that proves the owner is gone —
/// `EPERM` means it is alive under another user, so a run must not reclaim its root.
fn process_is_gone(pid: i32) -> bool {
    // SAFETY: `kill` with signal 0 only probes; it delivers nothing and touches no memory.
    let probe = unsafe { libc::kill(pid, 0) };
    probe != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

/// Detach every attached disk image whose backing path `select` accepts, using each image's first
/// (whole-device) `/dev/diskN` line — detaching the whole device takes its synthesized APFS
/// container and every mounted volume down with it. Text parsing is deliberate: this is
/// best-effort hygiene, and a parse miss only means the residue waits for the next sweep.
fn detach_images(select: impl Fn(&str) -> bool) {
    let output = match std::process::Command::new("hdiutil").arg("info").output() {
        Ok(output) => output,
        Err(error) => {
            eprintln!("hdiutil info could not run, so no scratch image was detached: {error}");
            return;
        }
    };
    let info = String::from_utf8_lossy(&output.stdout);
    let mut in_matching_image = false;
    for line in info.lines() {
        if let Some(image_path) = line.strip_prefix("image-path") {
            in_matching_image = select(image_path.trim_start_matches([' ', ':']));
            continue;
        }
        if !in_matching_image {
            continue;
        }
        if let Some(device) = line.split_whitespace().next().filter(|token| {
            token.starts_with("/dev/disk") && !token.trim_start_matches("/dev/disk").contains('s')
        }) {
            match std::process::Command::new("hdiutil")
                .args(["detach", device, "-force"])
                .output()
            {
                Ok(detached) if detached.status.success() => {}
                Ok(detached) => eprintln!(
                    "hdiutil detach {device} failed: {}",
                    String::from_utf8_lossy(&detached.stderr).trim()
                ),
                Err(error) => eprintln!("hdiutil detach {device} could not run: {error}"),
            }
            in_matching_image = false;
        }
    }
}
