//! What a declared land does after its turn, detached (02_workspaces.md "A declared land" step
//! 6): `<cowshed> __land-after <request>` pushes main and runs the repository's after-land
//! commands in order, each outcome one more `lands.log` line. The land starts it and returns: a
//! remote is never needed to land, and nothing it runs holds up this land or the next.

use std::ffi::OsString;
use std::fs::File;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use cowshed_core::declared_land::{self as declared, AfterRequest, LandEvent, LandRecord};
use cowshed_core::{CowshedError, Result};
use cowshed_os::fork_lock::Spawn as _;

/// The argument the after-land child is started with, before its request file.
pub const VERB: &str = "__land-after";

/// How long the push or an after command may write nothing before it is ended: its output is its
/// heartbeat, and nothing bounds its total (02_workspaces.md "A declared land" step 6).
const SILENCE: Duration = Duration::from_secs(180);

/// Start the after-land child for `request`, `<program> __land-after <request>`, and return at
/// once. The request is written beside
/// the turn's other land records, `lands/<head>.json`, and the child's own stderr to
/// `lands/<head>.log`; it runs in a session of its own, in `/`, with no descriptor of this
/// process but its stdio, so neither the land's reader nor its turn waits for it.
pub(crate) fn start(program: &Path, request: &AfterRequest) -> Result<PathBuf> {
    let directory = crate::declared_land::after_directory(&request.turn);
    std::fs::create_dir_all(&directory).map_err(|error| {
        CowshedError::internal(format!(
            "cannot create {} for the after-land record: {error}",
            directory.display()
        ))
    })?;
    let stem = request.head.as_str();
    let request_path = directory.join(format!("{stem}.json"));
    let log_path = directory.join(format!("{stem}.log"));
    let bytes = serde_json::to_vec_pretty(request).map_err(|error| {
        CowshedError::internal(format!("cannot encode the after-land request: {error}"))
    })?;
    std::fs::write(&request_path, bytes).map_err(|error| {
        CowshedError::internal(format!(
            "cannot write the after-land request {}: {error}",
            request_path.display()
        ))
    })?;
    let log = File::options()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|error| {
            CowshedError::internal(format!(
                "cannot open the after-land log {}: {error}",
                log_path.display()
            ))
        })?;
    let mut command = Command::new(program);
    command
        .arg(VERB)
        .arg(&request_path)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    // SAFETY: setsid is async-signal-safe and touches only the child.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    cowshed_core::exec::prepare_child_descriptors(&mut command).map_err(|failure| {
        CowshedError::internal(format!(
            "cannot seal the descriptors of the after-land child: {}",
            failure.source
        ))
    })?;
    let child = command.spawn_locked().map_err(|error| {
        CowshedError::internal(format!(
            "cannot start {} {VERB}: {error}",
            program.display()
        ))
    })?;
    // Collected whenever it ends while this process lives, and said; reparented once it exits.
    // Nothing waits for it: the land has already ended its turn.
    cowshed_os::deadline::collect_later("after-land child", child);
    Ok(log_path)
}

/// Run as the after-land child when the arguments say so, before anything else starts. `None`
/// when this process is not one.
pub fn dispatch() -> Option<i32> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next()? != VERB {
        return None;
    }
    let (Some(path), None) = (arguments.next(), arguments.next()) else {
        cowshed_os::eprintln!("cowshed: {VERB} takes exactly one request file");
        return Some(2);
    };
    let request: AfterRequest = match std::fs::read(&path)
        .map_err(|error| error.to_string())
        .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
    {
        Ok(request) => request,
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: {VERB} cannot read its request {}: {error}",
                Path::new(&path).display()
            );
            return Some(1);
        }
    };
    run(&request);
    Some(0)
}

/// Push, then each after command, in order; every outcome is recorded and none stops the next.
fn run(request: &AfterRequest) {
    let directory = crate::declared_land::after_directory(&request.turn);
    let head = request.head.as_str();
    if let Some(remote) = &request.push {
        let argv = [
            "git".to_owned(),
            "push".to_owned(),
            "--progress".to_owned(),
            remote.as_str().to_owned(),
            request.branch.clone(),
        ];
        let log = directory.join(format!("{head}.push.log"));
        let (event, detail) = match execute("after-land push", request, &argv, &log) {
            Ok(status) if status.success() => (
                LandEvent::Pushed,
                format!("{} {}", remote.as_str(), request.branch),
            ),
            Ok(status) => (
                LandEvent::PushFailed,
                format!(
                    "git push {} {} {}: {}; output in {}",
                    remote.as_str(),
                    request.branch,
                    crate::declared_land::describe(status),
                    last_line(&log),
                    log.display()
                ),
            ),
            Err(error) => (LandEvent::PushFailed, error),
        };
        note(request, event, detail);
    }
    for (index, argv) in request.after.iter().enumerate() {
        let log = directory.join(format!("{head}.after{}.log", index + 1));
        let detail = match execute("after-land command", request, argv, &log) {
            Ok(status) => format!(
                "{} {}; output in {}",
                argv.join(" "),
                crate::declared_land::describe(status),
                log.display()
            ),
            Err(error) => format!("{}: {error}", argv.join(" ")),
        };
        note(request, LandEvent::AfterExited, detail);
    }
}

/// Run one command, `role`, in main's pure environment, its output appended to `log` as it comes
/// and its silence bounded by [`SILENCE`].
fn execute(
    role: &'static str,
    request: &AfterRequest,
    argv: &[String],
    log: &Path,
) -> std::result::Result<std::process::ExitStatus, String> {
    let argv: Vec<OsString> = argv.iter().map(OsString::from).collect();
    let environment: Vec<(OsString, OsString)> = request
        .environment
        .iter()
        .map(|(name, value)| (OsString::from(name), OsString::from(value)))
        .collect();
    let output =
        File::create(log).map_err(|error| format!("cannot open {}: {error}", log.display()))?;
    let mut command = crate::declared_land::declared_command(&request.main, &argv, &environment)
        .map_err(|error| error.message)?;
    cowshed_os::deadline::run_to_log(role, &mut command, output, SILENCE)
        .map_err(|error| error.to_string())
}

fn note(request: &AfterRequest, event: LandEvent, detail: String) {
    let record = LandRecord::now(event, request.head.clone(), None, Some(detail));
    if let Err(error) = declared::record(&request.turn, &record) {
        cowshed_os::eprintln!(
            "cowshed: cannot record {:?} for {} in {}: {error}",
            record.event,
            request.head.as_str(),
            request.turn.join(declared::LANDS_LOG).display()
        );
    }
}

fn last_line(log: &Path) -> String {
    std::fs::read_to_string(log)
        .ok()
        .and_then(|text| {
            text.lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .map(str::to_owned)
        })
        .unwrap_or_default()
}
