//! A test run's fixture templates: a tree every test of one run would otherwise build for itself,
//! written once by whichever test asks first and cloned by every other (specs/cowshed/08_testing.md,
//! "Fixture templates").
//!
//! A fixture repository is a handful of git and file writes that every test repeated, and on the
//! loaded gate each git process took 0.8 s against 75 ms alone. A template holds no path of the
//! scratch it is cloned under, so one per run serves every test of the run. It lives in the run's
//! template directory ([`directory`]) and is written aside and moved in whole ([`materialized`]),
//! so a writer killed halfway leaves no template.
//!
//! Always compiled, as [`crate::test_backstop`] is: a dependant's tests take it through a plain
//! dependency, and a binary that calls none of it never links it.

use std::io;
use std::path::{Path, PathBuf};

/// The name of a run's short directories, `cowshed-dtest-<pid>-…`: a directory fixture's root and
/// the run's template directory. The lane setup's template and short-directory sweep takes such a
/// directory by the pid its name records, once no process holds that pid.
pub const ROOT_PREFIX: &str = "cowshed-dtest-";

/// The canonical temporary directory the run's short directories live in: `/tmp` resolves to
/// `/private/tmp` on Darwin.
#[cfg(target_os = "macos")]
const TEMPORARY: &str = "/private/tmp";
#[cfg(not(target_os = "macos"))]
const TEMPORARY: &str = "/tmp";

/// This run's template directory, named for the run's process ([`run_process`]): every test
/// process of the run writes and clones its templates there, and the lane setup's template sweep
/// takes it by that pid, so only once the run has ended.
pub fn directory() -> io::Result<PathBuf> {
    let directory = PathBuf::from(format!(
        "{TEMPORARY}/{ROOT_PREFIX}{}-templates",
        run_process()?
    ));
    std::fs::create_dir_all(&directory)
        .map_err(|error| on_path(error, "create the template directory", &directory))?;
    Ok(directory)
}

/// The process whose life is this test run: every test process of the run descends from it, and it
/// outlives them all. nextest names the run (`NEXTEST_RUN_ID`) and starts each test in a process
/// group of its own, led by whatever wraps the test (its target runner, `mbx-test-runner`, or a run
/// wrapper), which waits for it: the run's process is the nearest ancestor outside the test's
/// group, never the test's parent. Outside nextest a run is this one process.
pub fn run_process() -> io::Result<libc::pid_t> {
    // SAFETY: `getpid` has no preconditions and cannot fail.
    let this = unsafe { libc::getpid() };
    if std::env::var_os("NEXTEST_RUN_ID").is_none() {
        return Ok(this);
    }
    // SAFETY: `getpgrp` and `getppid` have no preconditions and cannot fail.
    let (group, mut ancestor) = unsafe { (libc::getpgrp(), libc::getppid()) };
    loop {
        let lineage = crate::process::lineage(ancestor)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("the run's process: ancestor {ancestor} of test process {this} is gone"),
            )
        })?;
        if lineage.pgid != group {
            return Ok(ancestor);
        }
        ancestor = lineage.ppid;
    }
}

/// The template `key` in `directory`, published once: under the template's lock, which every
/// caller takes, the first caller `write`s it aside and renames it in whole; every later caller
/// finds it there. An aside this process left, from a writer that panicked halfway, is removed
/// before `write` starts again: nothing else names it.
pub fn materialized(directory: &Path, key: &str, write: impl FnOnce(&Path)) -> io::Result<PathBuf> {
    let template = directory.join(key);
    let lock = directory.join(format!("{key}.lock"));
    let _writing = crate::test_backstop::lock_exclusive(&lock)
        .map_err(|error| on_path(error, "lock the template", &lock))?;
    if !template
        .try_exists()
        .map_err(|error| on_path(error, "look for the template", &template))?
    {
        let aside = directory.join(format!("{key}.{}", std::process::id()));
        match std::fs::remove_dir_all(&aside) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(on_path(error, "remove an abandoned template", &aside)),
        }
        write(&aside);
        std::fs::rename(&aside, &template)
            .map_err(|error| on_path(error, "publish the template", &template))?;
    }
    Ok(template)
}

/// `source`'s whole tree cloned to `destination`, which must not exist: one `clonefile`, which
/// copies no data.
#[cfg(target_os = "macos")]
pub fn clone_tree(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let path = |path: &Path| {
        std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{}: {error}", path.display()),
            )
        })
    };
    let (from, to) = (path(source)?, path(destination)?);
    // SAFETY: both arguments are NUL-terminated strings that outlive the call, which reads them
    // and touches no other memory.
    if unsafe { libc::clonefile(from.as_ptr(), to.as_ptr(), 0) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    Err(io::Error::new(
        error.kind(),
        format!(
            "clone {} to {}: {error}",
            source.display(),
            destination.display()
        ),
    ))
}

/// `source`'s whole tree copied to `destination`, which must not exist: each directory made
/// again, each file copied with its permissions (`copy_file_range`, which shares the file's
/// extents where the filesystem can), each link made again as the same link, and each
/// directory's own permissions set once its entries are in.
#[cfg(not(target_os = "macos"))]
pub fn clone_tree(source: &Path, destination: &Path) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(source)
        .map_err(|error| on_path(error, "read the template entry", source))?;
    if metadata.file_type().is_symlink() {
        let target = std::fs::read_link(source)
            .map_err(|error| on_path(error, "read the template link", source))?;
        std::os::unix::fs::symlink(&target, destination)
            .map_err(|error| on_path(error, "make the cloned link", destination))
    } else if metadata.is_dir() {
        std::fs::create_dir(destination)
            .map_err(|error| on_path(error, "make the cloned directory", destination))?;
        for entry in std::fs::read_dir(source)
            .map_err(|error| on_path(error, "read the template directory", source))?
        {
            let entry =
                entry.map_err(|error| on_path(error, "read the template directory", source))?;
            clone_tree(&entry.path(), &destination.join(entry.file_name()))?;
        }
        std::fs::set_permissions(destination, metadata.permissions())
            .map_err(|error| on_path(error, "set the cloned directory's mode", destination))
    } else {
        std::fs::copy(source, destination)
            .map(drop)
            .map_err(|error| on_path(error, "copy the template file", destination))
    }
}

fn on_path(error: io::Error, step: &str, path: &Path) -> io::Error {
    io::Error::new(error.kind(), format!("{step} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    use crate::test_backstop::CommandBackstop as _;

    /// Every test process of one run clones from one template directory, whatever wraps each
    /// test. nextest starts each test in a process group of its own, led by its target runner
    /// (`mbx-test-runner`) or run wrapper, which waits for the test: a test's parent is its own
    /// wrapper, never the run. Two test processes of one run, each under a wrapper of its own,
    /// name the same directory, named for this process, which plays the run and outlives both;
    /// the lane setup's template sweep takes a directory by the pid its name records, so only
    /// after the run.
    #[test]
    fn templates_are_one_directory_per_run_whatever_wraps_each_test() {
        use std::os::unix::process::CommandExt as _;

        const PROBE: &str = "COWSHED_TEST_TEMPLATE_DIRECTORY_PROBE";
        const MARK: &str = "template-directory=";
        if std::env::var_os(PROBE).is_some() {
            println!(
                "{MARK}{}",
                super::directory()
                    .expect("the run's template directory")
                    .display()
            );
            return;
        }
        let probe = || {
            // The shell leads the probe's process group and waits for it rather than exec it,
            // as nextest's target runner does for each test, so it is the probe's parent.
            let output = Command::new("/bin/sh")
                .args(["-c", "\"$0\" --exact \"$1\" --nocapture --quiet; exit $?"])
                .arg(std::env::current_exe().expect("test executable"))
                .arg("test_template::tests::templates_are_one_directory_per_run_whatever_wraps_each_test")
                .env(PROBE, "1")
                .env("NEXTEST_RUN_ID", "one-run-two-wrapped-tests")
                .process_group(0)
                .output_bounded()
                .expect("run a wrapped template probe");
            let stdout = String::from_utf8(output.stdout).expect("probe stdout");
            assert!(
                output.status.success(),
                "the probe failed\n{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            stdout
                .lines()
                .find_map(|line| line.strip_prefix(MARK))
                .map(PathBuf::from)
                .expect("the probe names its template directory")
        };
        let (first, second) = (probe(), probe());
        assert_eq!(
            first, second,
            "two test processes of one run share one template directory"
        );
        assert_eq!(
            first,
            PathBuf::from(format!(
                "{}/{}{}-templates",
                super::TEMPORARY,
                super::ROOT_PREFIX,
                std::process::id()
            )),
            "the directory is named for the run's process, which outlives every test of the run"
        );
    }
}
