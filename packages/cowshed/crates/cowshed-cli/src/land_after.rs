//! What a declared land does after its turn (02_workspaces.md "A declared land" step 6):
//! `<cowshed> __land-after <request>` pushes main and runs the repository's after-land commands
//! in order, each outcome one more `lands.log` line. The land submits it to main's workspace
//! supervisor as a background job and returns ([`argv`]): a remote is never needed to land, and
//! nothing it runs holds up this land or the next.

use std::ffi::OsString;
use std::fs::File;
use std::path::Path;
use std::time::Duration;

use cowshed_core::declared_land::{self as declared, AfterRequest, LandEvent, LandRecord};
use cowshed_core::{CowshedError, Result};

/// The argument the after-land child is started with, before its request file.
pub const VERB: &str = "__land-after";

/// How long the push or an after command may write nothing before it is ended: its output is its
/// heartbeat, and nothing bounds its total (02_workspaces.md "A declared land" step 6).
const SILENCE: Duration = Duration::from_secs(180);

/// Write `request` beside the turn's other land records, `lands/<head>.json`, and answer the
/// argv of its job: `<program> __land-after <request>`, which main's supervisor runs.
pub(crate) fn argv(program: &Path, request: &AfterRequest) -> Result<Vec<OsString>> {
    let directory = crate::declared_land::after_directory(&request.turn);
    std::fs::create_dir_all(&directory).map_err(|error| {
        CowshedError::internal(format!(
            "cannot create {} for the after-land record: {error}",
            directory.display()
        ))
    })?;
    let request_path = directory.join(format!("{}.json", request.head.as_str()));
    let bytes = serde_json::to_vec_pretty(request).map_err(|error| {
        CowshedError::internal(format!("cannot encode the after-land request: {error}"))
    })?;
    std::fs::write(&request_path, bytes).map_err(|error| {
        CowshedError::internal(format!(
            "cannot write the after-land request {}: {error}",
            request_path.display()
        ))
    })?;
    Ok(vec![
        program.as_os_str().to_owned(),
        OsString::from(VERB),
        request_path.into_os_string(),
    ])
}
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
        let Executed { ran, unretired } = execute("after-land push", request, &argv, &log);
        let (event, detail) = match ran {
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
        note(request, event, beside(unretired, detail));
    }
    for (index, argv) in request.after.iter().enumerate() {
        let log = directory.join(format!("{head}.after{}.log", index + 1));
        let Executed { ran, unretired } = execute("after-land command", request, argv, &log);
        let detail = match ran {
            Ok(status) => format!(
                "{} {}; output in {}",
                argv.join(" "),
                crate::declared_land::describe(status),
                log.display()
            ),
            Err(error) => format!("{}: {error}", argv.join(" ")),
        };
        note(request, LandEvent::AfterExited, beside(unretired, detail));
    }
}

/// One command's run: its own outcome, and why its cgroup leaf could not be retired when it could
/// not (Linux), said beside that outcome and never in its place.
struct Executed {
    ran: std::result::Result<std::process::ExitStatus, String>,
    unretired: Option<String>,
}

/// `detail`, with the leaf's retirement failure after it when there was one.
fn beside(unretired: Option<String>, detail: String) -> String {
    match unretired {
        Some(unretired) => format!("{detail}; its cgroup leaf was not retired: {unretired}"),
        None => detail,
    }
}
/// Run one command, `role`, in main's pure environment, its output appended to `log` as it comes
/// and its silence bounded by [`SILENCE`]. Its members are its own process group on Darwin, and on
/// Linux a cgroup leaf of its own inside this after-land job's cgroup, named for its log and
/// retired once it ends, so nothing an earlier command left behind is counted as this one's.
fn execute(role: &'static str, request: &AfterRequest, argv: &[String], log: &Path) -> Executed {
    let ran = |ran| Executed {
        ran,
        unretired: None,
    };
    let argv: Vec<OsString> = argv.iter().map(OsString::from).collect();
    let environment: Vec<(OsString, OsString)> = request
        .environment
        .iter()
        .map(|(name, value)| (OsString::from(name), OsString::from(value)))
        .collect();
    let output = match File::create(log) {
        Ok(output) => output,
        Err(error) => return ran(Err(format!("cannot open {}: {error}", log.display()))),
    };
    let mut command =
        match crate::declared_land::declared_command(&request.main, &argv, &environment) {
            Ok(command) => command,
            Err(error) => return ran(Err(error.message)),
        };
    #[cfg(target_os = "macos")]
    return ran(cowshed_os::deadline::run_to_log(
        role,
        &mut command,
        cowshed_os::deadline::Members::OwnGroup,
        output,
        SILENCE,
    )
    .map_err(|error| error.to_string()));
    #[cfg(target_os = "linux")]
    {
        let own = match cowshed_os::process::own_cgroup() {
            Ok(own) => own,
            Err(error) => return ran(Err(error.to_string())),
        };
        let name = format!(
            "after-{}",
            log.file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned())
        );
        let leaf = match cowshed_core::runtime::job_cgroup::RunCgroup::inside(&own, &name) {
            Ok(leaf) => leaf,
            Err(error) => return ran(Err(error.to_string())),
        };
        let outcome = cowshed_os::deadline::run_to_log(
            role,
            &mut command,
            cowshed_os::deadline::Members::Leaf {
                cgroup: leaf.directory(),
                path: leaf.path(),
            },
            output,
            SILENCE,
        )
        .map_err(|error| error.to_string());
        Executed {
            ran: outcome,
            unretired: leaf
                .retire(cowshed_os::deadline::REAP)
                .err()
                .map(|error| error.to_string()),
        }
    }
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
