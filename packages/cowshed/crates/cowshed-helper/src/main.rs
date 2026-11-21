//! `cowshed-helper <root> <verb> …`: the root binary through which cowshed mutates ZFS datasets
//! (specs/cowshed/09_substrates.md, "Privileged helper").
//!
//! It runs at most one `zfs` command per invocation, from a fixed verb set ([`verb::plan`]), and
//! only
//! - for a root dataset that names the caller: `<root>` is `<parent>/cowshed` and carries the
//!   local user property `cowshed:owner=<caller uid>`, which only the administrator of its parent
//!   can set;
//! - on datasets strictly below that root ([`name::Root::holds`]);
//! - with properties from an allow-list, and mountpoints in a tree the caller owns
//!   ([`mountpoint::permit`]).
//!
//! Reads (`zfs list`, `zfs get`) need no privilege and never come here. `check` runs no command:
//! it answers 0 once the helper runs as root and the root names the caller, which is how an
//! install is proven before anything depends on it.

mod mountpoint;
mod verb;

use std::fmt;
use std::fs::File;
use std::os::unix::fs::OpenOptionsExt;
use std::process::{Command, ExitCode, Output, Stdio};

use cowshed_helper::host;
use cowshed_helper::name::{Dataset, Root};
use verb::{OWNER_PROPERTY, Plan};

/// The exit status of a refusal: nothing ran. A failed `zfs` exits with `zfs`'s own status.
const REFUSED: u8 = 2;

enum Failure {
    Refused(String),
    Zfs {
        arguments: Vec<String>,
        output: Output,
    },
    Io {
        doing: String,
        source: std::io::Error,
    },
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(reason) => write!(f, "refused: {reason}"),
            Self::Zfs { arguments, output } => write!(
                f,
                "`zfs {}` failed ({}): {}",
                arguments.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim_end()
            ),
            Self::Io { doing, source } => write!(f, "{doing}: {source}"),
        }
    }
}

fn refused(reason: impl fmt::Display) -> Failure {
    Failure::Refused(reason.to_string())
}

/// Run `zfs <arguments>` with nothing of the caller's environment, and collect it.
fn zfs(arguments: &[String]) -> Result<Output, Failure> {
    // WHY unbounded: one `zfs` command on datasets the caller named; the caller bounds the
    // helper it started, and a root child cut short mid-transaction would leave less than `zfs`
    // itself guarantees.
    let output = Command::new("zfs")
        .args(arguments)
        .env_clear()
        .env("PATH", host::TRUSTED_PATH)
        .stdin(Stdio::null())
        .output()
        .map_err(|source| Failure::Io {
            doing: format!("run zfs from {}", host::TRUSTED_PATH),
            source,
        })?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(Failure::Zfs {
            arguments: arguments.to_vec(),
            output,
        })
    }
}

/// `value<TAB>source` of each of `properties` on `dataset`, in order.
fn properties(dataset: &Dataset, properties: &str) -> Result<Vec<(String, String)>, Failure> {
    let arguments = [
        "get",
        "-H",
        "-p",
        "-o",
        "value,source",
        properties,
        dataset.as_str(),
    ]
    .map(str::to_owned);
    let output = zfs(&arguments)?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .map(|(value, source)| (value.to_owned(), source.to_owned()))
        .collect())
}

/// Refuse unless `root` itself says cowshed serves `uid` there.
fn authorize(root: &Root, uid: u32) -> Result<(), Failure> {
    let owner = properties(root.dataset(), OWNER_PROPERTY)?;
    match owner.as_slice() {
        [(value, source)] if *value == uid.to_string() && source == "local" => Ok(()),
        [(value, source)] => Err(refused(format_args!(
            "{} has {OWNER_PROPERTY}={value} ({source}); it must be set locally to the caller's uid, {uid}",
            root.dataset()
        ))),
        _ => Err(refused(format_args!(
            "cannot read {OWNER_PROPERTY} of {}",
            root.dataset()
        ))),
    }
}

/// Give the mounted root of the dataset `create` just made to the caller. The directory is
/// opened once and is only handed over when the kernel says that descriptor is the root of this
/// dataset's mount, so no path the caller can rearrange decides what changes owner.
fn own(dataset: &Dataset, uid: u32, gid: u32) -> Result<(), Failure> {
    let read = properties(dataset, "mounted,mountpoint")?;
    let [(mounted, _), (mountpoint, _)] = read.as_slice() else {
        return Err(refused(format_args!("cannot read the mount of {dataset}")));
    };
    if mounted != "yes" {
        return Ok(());
    }
    let io = |doing: &str| {
        let doing = format!("{doing} {mountpoint}");
        move |source| Failure::Io { doing, source }
    };
    let directory = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(mountpoint)
        .map_err(io("open"))?;
    match host::mounted_dataset(&directory).map_err(io("read the mount at"))? {
        Some(source) if source == dataset.as_str() => {
            host::give(&directory, uid, gid).map_err(io("change the owner of"))
        }
        other => Err(refused(format_args!(
            "{mountpoint} is the mounted root of {other:?}, not of {dataset}"
        ))),
    }
}

fn run(arguments: &[String]) -> Result<(), Failure> {
    let (root, verb) = arguments
        .split_first()
        .ok_or_else(|| refused(verb::USAGE))?;
    if !host::is_root() {
        return Err(refused(
            "cowshed-helper must run as root (setuid root, or through sudo)",
        ));
    }
    let root = Root::parse(root).map_err(|error| refused(format_args!("{root:?}: {error}")))?;
    let Plan {
        zfs: command,
        mountpoints,
        own: created,
    } = verb::plan(&root, verb).map_err(refused)?;
    let (uid, gid) = host::caller();
    authorize(&root, uid)?;
    for mountpoint in &mountpoints {
        mountpoint::permit(mountpoint, uid).map_err(refused)?;
    }
    if let Some(command) = &command {
        zfs(command)?;
    }
    match created {
        Some(dataset) => own(&dataset, uid, gid),
        None => Ok(()),
    }
}

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match run(&arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            #[allow(clippy::print_stderr)] // The helper's whole report is this one line.
            {
                eprintln!("cowshed-helper: {failure}");
            }
            match failure {
                Failure::Zfs { output, .. } => output
                    .status
                    .code()
                    .and_then(|code| u8::try_from(code).ok())
                    .map_or(ExitCode::FAILURE, ExitCode::from),
                Failure::Refused(_) => ExitCode::from(REFUSED),
                Failure::Io { .. } => ExitCode::FAILURE,
            }
        }
    }
}
