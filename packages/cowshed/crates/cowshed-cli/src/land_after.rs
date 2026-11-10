//! What a declared land does after its turn (02_workspaces.md "A declared land" step 6):
//! `<cowshed> __land-after <request>` runs the repository's after-land commands in order and then
//! pushes main, each outcome one more `lands.log` line. The land submits it to main's workspace
//! supervisor as a background job and returns ([`argv`]): a remote is never needed to land, and
//! nothing it runs holds up this land or the next.

use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cowshed_core::declared_land::{self as declared, AfterRequest, LandEvent, LandRecord};
use cowshed_core::{CowshedError, Result};

use crate::gateway_service::InstalledBuild;
use crate::launchd::HostStableExecutable;

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
/// Run the after-land work when this process is `__land-after`: its exit code, or `None`
/// when this process is not one.
pub fn dispatch() -> Option<i32> {
    let mut arguments = std::env::args_os();
    // The program the land started this job as: the installed cowshed's stable name.
    let program = PathBuf::from(arguments.next()?);
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
    run(&request, &program);
    Some(0)
}

/// Each after command in order, then the push; every outcome is recorded and none stops the next.
/// The after commands rebuild main's outputs for the sources the land just checked out, and until
/// they end main's sources are newer than the builds its developer links serve, so they go first:
/// a remote is never needed by a local consumer, and the push waits for nothing else.
fn run(request: &AfterRequest, program: &Path) {
    let directory = crate::declared_land::after_directory(&request.turn);
    let head = request.head.as_str();
    // The installed cowshed this job runs as: an after command that installs replaces it.
    let installed = HostStableExecutable::at(program);
    if installed.is_none() {
        cowshed_os::eprintln!(
            "cowshed: {VERB} runs as {}, which is no installed cowshed's stable name; an install an \
             after-land command makes is not recorded",
            program.display()
        );
    }
    for (index, argv) in request.after.iter().enumerate() {
        let log = directory.join(format!("{head}.after{}.log", index + 1));
        let before = installed.as_ref().map(read_installed);
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
        if let (Some(executable), Some(before)) = (&installed, before) {
            note_install(request, argv, before, read_installed(executable));
        }
    }
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
}

/// What the stable name runs, or why it cannot be read.
fn read_installed(
    executable: &HostStableExecutable,
) -> std::result::Result<Option<InstalledBuild>, String> {
    crate::gateway_service::installed_build(executable).map_err(|error| error.message)
}

/// Record that `argv` installed cowshed when the stable name runs another build after it than
/// before it (05_gateway.md "A land installs its own cowshed after its turn"); say on stderr when
/// either read failed, so it cannot be told.
fn note_install(
    request: &AfterRequest,
    argv: &[String],
    before: std::result::Result<Option<InstalledBuild>, String>,
    after: std::result::Result<Option<InstalledBuild>, String>,
) {
    match (before, after) {
        (Ok(before), Ok(after)) => {
            if let Some(detail) = installed_detail(before.as_ref(), after.as_ref(), argv) {
                note(request, LandEvent::Installed, detail);
            }
        }
        (Err(error), _) | (_, Err(error)) => cowshed_os::eprintln!(
            "cowshed: cannot tell whether {} installed cowshed: {error}",
            argv.join(" ")
        ),
    }
}

/// The detail of an `installed` record: `sha256 <digest> commit <commit>; …`, the stored copy the
/// stable name runs after `argv` and the commit its build record names (`unrecorded` for a build
/// that records none). `None` when the stable name runs the build it ran before, or none.
fn installed_detail(
    before: Option<&InstalledBuild>,
    after: Option<&InstalledBuild>,
    argv: &[String],
) -> Option<String> {
    let after = after?;
    if before.is_some_and(|before| before.digest == after.digest) {
        return None;
    }
    Some(format!(
        "sha256 {} commit {}; the installed cowshed changed while {} ran",
        after.digest,
        after
            .record
            .as_ref()
            .map_or("unrecorded", |record| record.commit.as_str()),
        argv.join(" ")
    ))
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    #[cfg(target_os = "macos")]
    use std::path::PathBuf;

    #[cfg(target_os = "macos")]
    use cowshed_core::api::dto::GitOid;
    #[cfg(target_os = "macos")]
    use cowshed_core::declared_land::{AfterRequest, LandEvent, LandRecord};

    use crate::build_record::BuildRecord;
    use crate::gateway_service::InstalledBuild;
    use crate::launchd::{ExecutableDigest, HostStableExecutable};

    #[cfg(target_os = "macos")]
    const BEFORE: &str = "1111111111111111111111111111111111111111";
    #[cfg(target_os = "macos")]
    const HEAD: &str = "2222222222222222222222222222222222222222";
    const INSTALLED_COMMIT: &str = "3333333333333333333333333333333333333333";
    const OLD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const NEW: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    /// A home whose installed cowshed's stable name says `cowshed-<OLD>`, as an install leaves it.
    #[cfg(target_os = "macos")]
    fn installed_home(root: &Path) -> PathBuf {
        let home = root.join("home");
        let bin = cowshed_core::host_dirs::installed_binary_directory(&home);
        std::fs::create_dir_all(&bin).expect("the installed binary directory");
        std::fs::write(bin.join(format!("cowshed-{OLD}")), b"old").expect("the old copy");
        std::os::unix::fs::symlink(format!("cowshed-{OLD}"), bin.join("cowshed"))
            .expect("the stable name");
        bin.join("cowshed")
    }

    fn build(digest: &str, commit: Option<&str>) -> InstalledBuild {
        InstalledBuild {
            digest: ExecutableDigest::parse(digest).expect("a digest"),
            record: commit.map(|commit| BuildRecord {
                commit: commit.to_owned(),
                commit_time: 1,
            }),
        }
    }

    /// Only a stable name that runs another build after a command than before it is an install,
    /// and its record names that build's digest and commit, or says it recorded none.
    #[test]
    fn an_install_is_a_changed_build_under_the_stable_name() {
        let argv = ["sh".to_owned(), "tooling/land-install.sh".to_owned()];
        let old = build(OLD, Some(INSTALLED_COMMIT));
        let new = build(NEW, Some(INSTALLED_COMMIT));
        assert_eq!(super::installed_detail(Some(&old), Some(&old), &argv), None);
        assert_eq!(super::installed_detail(Some(&old), None, &argv), None);
        assert_eq!(
            super::installed_detail(Some(&old), Some(&new), &argv).as_deref(),
            Some(
                format!(
                    "sha256 {NEW} commit {INSTALLED_COMMIT}; the installed cowshed changed while \
                     sh tooling/land-install.sh ran"
                )
                .as_str()
            )
        );
        assert_eq!(
            super::installed_detail(None, Some(&build(NEW, None)), &argv).as_deref(),
            Some(
                format!(
                    "sha256 {NEW} commit unrecorded; the installed cowshed changed while sh \
                     tooling/land-install.sh ran"
                )
                .as_str()
            )
        );
    }

    /// The program an after-land job runs as is the stable name of the home it derives from; any
    /// other path is none.
    #[test]
    fn the_stable_name_is_known_by_its_path() {
        let home = Path::new("/Users/someone");
        let stable = HostStableExecutable::new(home, "cowshed").expect("a stable name");
        assert_eq!(
            HostStableExecutable::at(stable.path()),
            Some(stable.clone())
        );
        assert_eq!(
            HostStableExecutable::at(&stable.directory().join("cowshed-old").join("cowshed")),
            None
        );
        assert_eq!(
            HostStableExecutable::at(Path::new("/usr/local/bin/cowshed")),
            None
        );
    }

    #[cfg(target_os = "macos")]
    fn lands(turn: &Path) -> Vec<LandRecord> {
        std::fs::read_to_string(turn.join(cowshed_core::declared_land::LANDS_LOG))
            .expect("lands.log")
            .lines()
            .map(|line| serde_json::from_str(line).expect("a lands.log line"))
            .collect()
    }

    /// The after-step that installs cowshed says so in the land's `lands.log`, through the writer
    /// of the land's other events, with the installed copy's SHA-256 and the commit its build
    /// record names (05_gateway.md "A land installs its own cowshed after its turn"). An after
    /// command that installs nothing records only its exit.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_after_command_that_installs_cowshed_records_the_installed_build() {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cs-land-after",
        );
        // The stable name is derived from a canonical home, as the host's is.
        let root = scratch.canonicalize().expect("the canonical test root");
        let program = installed_home(&root);
        let bin = program.parent().expect("the installed binary directory");
        let main = root.join("main");
        std::fs::create_dir(&main).expect("main");
        let turn = root.join("turn");
        let install = format!(
            "printf new > cowshed-{NEW} && printf '{{\"commit\":\"{INSTALLED_COMMIT}\",\"commitTime\":1}}' > cowshed-{NEW}.build && ln -sfh cowshed-{NEW} cowshed"
        );
        let request = AfterRequest {
            main: main.clone(),
            turn: turn.clone(),
            branch: "main".to_owned(),
            before: GitOid::new(BEFORE).expect("an oid"),
            head: GitOid::new(HEAD).expect("an oid"),
            push: None,
            after: vec![
                vec!["/bin/sh".to_owned(), "-c".to_owned(), "true".to_owned()],
                vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    format!("cd '{}' && {install}", bin.display()),
                ],
            ],
            environment: vec![("PATH".to_owned(), "/usr/bin:/bin".to_owned())],
        };

        super::argv(&program, &request).expect("the land writes the request beside its records");
        super::run(&request, &program);

        let lands = lands(&turn);
        assert_eq!(
            lands.iter().map(|record| record.event).collect::<Vec<_>>(),
            [
                LandEvent::AfterExited,
                LandEvent::AfterExited,
                LandEvent::Installed
            ],
            "{lands:?}"
        );
        let installed = &lands[2];
        assert_eq!(installed.head.as_str(), HEAD);
        let detail = installed.detail.as_deref().expect("the installed build");
        assert!(
            detail.starts_with(&format!("sha256 {NEW} commit {INSTALLED_COMMIT}; ")),
            "{detail}"
        );
    }
}
