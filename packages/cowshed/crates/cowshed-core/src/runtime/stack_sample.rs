//! The unprivileged macOS stack sampler a wait recording takes a silent workspace supervisor's or
//! cowshed controller's stacks with ([`super::wait_spans::WaitRecording`]).

#[cfg(target_os = "macos")]
pub fn capture(pid: i32) -> Result<Vec<u8>, String> {
    use cowshed_os::deadline::{self, Bound, LatencyClass};
    // Two seconds of stacks, with the existing 10-second ffi class for the helper's whole run.
    let bound = Bound::new(LatencyClass::Ffi, deadline::LAUNCHCTL.within);
    let report = std::env::temp_dir().join(format!(
        "cowshed-sample-{pid}-{}.txt",
        uuid::Uuid::new_v4().simple()
    ));
    report_within(
        std::process::Command::new("/usr/bin/sample")
            .arg(pid.to_string())
            .args(["2", "-mayDie", "-file"])
            .arg(&report),
        &report,
        &deadline::start("sample silent peer", bound),
    )
    .map_err(|unread| format!("sample pid {pid}: {unread}"))
}

/// Why a sampler's report was not read.
#[cfg(target_os = "macos")]
#[derive(Debug)]
enum Unread {
    /// The sampler had not ended at its bound and was left running: `report` holds its stacks
    /// once it ends.
    Late {
        report: std::path::PathBuf,
        bound: cowshed_os::deadline::RunError,
    },
    Failed(String),
}

#[cfg(target_os = "macos")]
impl std::fmt::Display for Unread {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Late { report, bound } => write!(
                formatter,
                "{bound}; the sampler was left running and its report is {} once it ends",
                report.display()
            ),
            Self::Failed(why) => formatter.write_str(why),
        }
    }
}

/// Run `sampler`, which writes its stacks to `report`, to its end within `deadline`, and take
/// the report. `report` is made here, private to this user, and removed once it is read. A
/// sampler past its bound keeps the file: it writes it when it ends, and the failure names it.
#[cfg(target_os = "macos")]
fn report_within(
    sampler: &mut std::process::Command,
    report: &std::path::Path,
    deadline: &cowshed_os::deadline::Deadline,
) -> Result<Vec<u8>, Unread> {
    use cowshed_os::deadline::RunError;
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(report)
        .map_err(|error| {
            Unread::Failed(format!(
                "its report {} could not be created: {error}",
                report.display()
            ))
        })?;
    let remove = || {
        if let Err(error) = std::fs::remove_file(report)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            cowshed_os::eprintln!(
                "cowshed: the sampler's report {} stays: {error}",
                report.display()
            );
        }
    };
    let output =
        match cowshed_os::deadline::output_within("silent peer stack sampler", sampler, deadline) {
            Ok(output) => output,
            Err(bound @ RunError::Reap(_)) => {
                return Err(Unread::Late {
                    report: report.to_owned(),
                    bound,
                });
            }
            Err(error) => {
                remove();
                return Err(Unread::Failed(error.to_string()));
            }
        };
    let stacks = std::fs::read(report);
    remove();
    let said = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return Err(Unread::Failed(format!("ended {}: {said}", output.status)));
    }
    match stacks {
        Ok(stacks) if stacks.is_empty() => {
            Err(Unread::Failed(format!("returned no stacks: {said}")))
        }
        Ok(stacks) => Ok(stacks),
        Err(error) => Err(Unread::Failed(format!(
            "its report {} could not be read: {error}",
            report.display()
        ))),
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::{Unread, report_within};
    use cowshed_os::deadline::{self, Bound, LatencyClass};
    use std::os::unix::ffi::OsStrExt as _;

    fn fifo(path: &std::path::Path) {
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("a path");
        // SAFETY: `name` is a NUL-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0, "mkfifo");
    }

    //#region smoo/cowshed!n/late-sampler-report
    /// A sampler that has not ended at its bound is left running, and the failure names the
    /// report it writes (05_gateway.md, "Deadlines"): the stand-in sampler is held at a FIFO
    /// past a zero bound, and once released its stacks are in the file the failure named.
    #[test]
    fn a_sampler_past_its_bound_leaves_its_stacks_where_the_failure_names_them() {
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cs-sample");
        let (gate, done, report) = (root.join("gate"), root.join("done"), root.join("report"));
        fifo(&gate);
        fifo(&done);
        let unread = report_within(
            std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    r#"read line < "$1"; printf stacks > "$2"; : > "$3""#,
                    "sh",
                ])
                .arg(&gate)
                .arg(&report)
                .arg(&done),
            &report,
            &deadline::start(
                "sample a held stand-in",
                Bound::new(LatencyClass::Ffi, std::time::Duration::ZERO),
            ),
        );
        let Err(Unread::Late { report: named, .. }) = unread else {
            panic!("a sampler held past its bound is late: {unread:?}");
        };
        assert_eq!(named, report);
        // Each open waits for the stand-in's own: its read of the gate, then its last write.
        std::fs::write(&gate, "go\n").expect("release the sampler");
        std::fs::read(&done).expect("the sampler's end");
        assert_eq!(std::fs::read(&report).expect("the late report"), b"stacks");
    }

    /// A sampler that ends within its bound answers its stacks and leaves no file.
    #[test]
    fn a_sampler_within_its_bound_answers_its_stacks_and_removes_its_report() {
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cs-sample");
        let report = root.join("report");
        let stacks = report_within(
            std::process::Command::new("/bin/sh")
                .args(["-c", r#"printf stacks > "$1""#, "sh"])
                .arg(&report),
            &report,
            &deadline::start("sample a stand-in", deadline::LAUNCHCTL),
        )
        .expect("the stand-in's stacks");
        assert_eq!(stacks, b"stacks");
        assert!(!report.exists(), "the read report is removed");
    }
    //#endregion smoo/cowshed!n/late-sampler-report
}

#[cfg(not(target_os = "macos"))]
pub fn capture(pid: i32) -> Result<Vec<u8>, String> {
    Err(format!(
        "stack sampling pid {pid} is unsupported on this platform"
    ))
}
