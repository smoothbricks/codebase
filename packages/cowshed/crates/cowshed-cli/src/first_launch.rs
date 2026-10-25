//! A fresh executable's first launch against its second: whether macOS scans new executables a
//! process runs (05_gateway.md "The gateway host", 06_cli.md `doctor`).
//!
//! macOS assesses each new executable on its first launch and charges the wait to the launching
//! run's responsible process; a responsible process with a Developer Tools entry is exempt. The
//! TCC database that holds the entries is unreadable to the user, so this reads behavior: a
//! fresh copy of a program, launched twice from one process. A charged first launch pays the
//! scan and the second does not; an exempt one costs what the second does. `cowshed doctor`
//! measures it in its own process (the host shell) and in the gateway daemon's, whose
//! responsible process is every workspace job's.

use std::io;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use cowshed_gateway_types::{FirstLaunch, FirstLaunchProbe};
use cowshed_os::fork_lock::Run as _;

use crate::code_signing::CODESIGN;

/// Mint a fresh executable from `template` and time its first and second launch from this
/// process, each from spawn to exit.
///
/// Fresh means what an archive extraction or a link makes: a new file, and a new code
/// signature — the copy is signed ad hoc again under an identifier no other launch has used —
/// so no assessment of the template or of an earlier probe answers for it. The probe runs
/// `--version`, so its launch is the program's start and nothing else. The scratch directory is
/// removed on every exit path.
pub fn measure(template: &Path) -> io::Result<FirstLaunch> {
    let scratch = Scratch::create()?;
    let probe = scratch.0.join("probe");
    std::fs::copy(template, &probe).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "copy {} to {}: {error}",
                template.display(),
                probe.display()
            ),
        )
    })?;
    let signed = Command::new(CODESIGN)
        .args(["--force", "--sign", "-", "--identifier"])
        .arg(format!("dev.cowshed.first-launch.{}", scratch.1))
        .arg(&probe)
        .output_locked()?;
    if !signed.status.success() {
        return Err(io::Error::other(format!(
            "{CODESIGN} could not sign the probe {} ({}): {}",
            probe.display(),
            signed.status,
            String::from_utf8_lossy(&signed.stderr).trim()
        )));
    }
    let first = launch(&probe)?;
    let second = launch(&probe)?;
    Ok(FirstLaunch {
        first_micros: first,
        second_micros: second,
    })
}

fn launch(probe: &Path) -> io::Result<u64> {
    let started = Instant::now();
    let status = Command::new(probe)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status_locked()?;
    let elapsed = started.elapsed();
    if !status.success() {
        return Err(io::Error::other(format!(
            "the probe {} --version ended {status}",
            probe.display()
        )));
    }
    Ok(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX))
}

/// A private directory of this process's own, and the nonce that names it.
struct Scratch(PathBuf, u128);

impl Scratch {
    fn create() -> io::Result<Self> {
        use std::os::unix::fs::DirBuilderExt as _;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "cowshed-first-launch-{}-{nonce}",
            std::process::id()
        ));
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path, nonce))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            cowshed_os::eprintln!(
                "cowshed: could not remove the first-launch probe {}: {error}",
                self.0.display()
            );
        }
    }
}

/// The daemon's [`FirstLaunchProbe`]: [`measure`] with the daemon's own executable as the
/// template, run in the daemon's process.
#[derive(Debug)]
pub struct DaemonFirstLaunch {
    template: PathBuf,
}

impl DaemonFirstLaunch {
    pub fn new(template: PathBuf) -> Self {
        Self { template }
    }
}

impl FirstLaunchProbe for DaemonFirstLaunch {
    fn measure(&self) -> Result<FirstLaunch, String> {
        measure(&self.template).map_err(|error| error.to_string())
    }
}

/// The program macOS charges this process's launches to: its responsible process's executable,
/// as libquarantine's `responsibility_get_pid_responsible_for_pid` and `proc_pidpath` name it.
/// `None` when either does not answer.
#[cfg(target_os = "macos")]
pub fn responsible_program() -> Option<PathBuf> {
    type Responsible = unsafe extern "C" fn(libc::pid_t) -> libc::pid_t;
    // SAFETY: a NUL-terminated symbol name looked up in the process's loaded images.
    let symbol = unsafe {
        libc::dlsym(
            libc::RTLD_DEFAULT,
            c"responsibility_get_pid_responsible_for_pid".as_ptr(),
        )
    };
    if symbol.is_null() {
        return None;
    }
    // SAFETY: the symbol is libquarantine's `pid_t (pid_t)` function.
    let responsible = unsafe { std::mem::transmute::<*mut libc::c_void, Responsible>(symbol) };
    // SAFETY: it takes and returns a pid and touches no memory of ours.
    let pid = unsafe { responsible(libc::getpid()) };
    if pid <= 0 {
        return None;
    }
    let capacity = u32::try_from(libc::PROC_PIDPATHINFO_MAXSIZE).ok()?;
    let mut path = vec![0u8; usize::try_from(capacity).ok()?];
    // SAFETY: `path` is a writable buffer of the length passed.
    let length = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), capacity) };
    let length = usize::try_from(length).ok().filter(|length| *length > 0)?;
    path.truncate(length);
    Some(PathBuf::from(std::ffi::OsString::from_vec(path)))
}

#[cfg(not(target_os = "macos"))]
pub fn responsible_program() -> Option<PathBuf> {
    None
}

/// Whether a responsible process's runs pay the first-launch scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exemption {
    /// Its fresh executables start as fast as the host shell's.
    Exempt,
    /// Its fresh executables wait for the scan.
    Charged,
}

/// Judge the gateway's first launch against the host shell's.
///
/// What a first launch costs beyond its second is the scan, plus noise. The host shell is the
/// reference: its terminal holds the exemption, so its excess is what an exempt run pays on this
/// host, at this load. The gateway is exempt while its excess stays within the host shell's
/// excess plus one host-shell first launch. A charged launch of a program cowshed's size waits
/// hundreds of milliseconds to seconds, which no host-shell spread approaches.
pub fn judge(host: FirstLaunch, gateway: FirstLaunch) -> Exemption {
    let excess = |launch: FirstLaunch| launch.first_micros.saturating_sub(launch.second_micros);
    if excess(gateway) <= excess(host).saturating_add(host.first_micros) {
        Exemption::Exempt
    } else {
        Exemption::Charged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn launch(first_ms: u64, second_ms: u64) -> FirstLaunch {
        FirstLaunch {
            first_micros: first_ms * 1000,
            second_micros: second_ms * 1000,
        }
    }

    /// The table item 61 measured: a host shell at 25 ms and 27 ms, the gateway's first launch
    /// 0.2 s to 17.6 s against a second at host-shell speed.
    #[test]
    fn a_first_launch_that_waits_for_the_scan_is_charged() {
        let host = launch(25, 27);
        assert_eq!(judge(host, launch(17_600, 22)), Exemption::Charged);
        assert_eq!(judge(host, launch(200, 22)), Exemption::Charged);
    }

    #[test]
    fn a_first_launch_within_the_host_shells_spread_is_exempt() {
        let host = launch(15, 10);
        assert_eq!(judge(host, launch(14, 11)), Exemption::Exempt);
        assert_eq!(judge(host, launch(30, 11)), Exemption::Exempt);
        assert_eq!(judge(host, launch(31, 10)), Exemption::Charged);
        // A second launch slower than the first is no excess at all.
        assert_eq!(judge(host, launch(9, 40)), Exemption::Exempt);
    }
}
