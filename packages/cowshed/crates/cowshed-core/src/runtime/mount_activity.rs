//! Who holds a workspace's mount beyond the processes cowshed runs on it (02_workspaces.md,
//! "`cowshed rm <ws>`" step 2 and "`cowshed attach` / `cowshed detach`"; 11_shell.md,
//! "Teardown ordering"). A host shell's `direnv exec <shed> nx …` runs on the mount with no
//! supervisor and no job: its working directory and its open files are the only sign of it. A
//! detach or a retirement asks for them before it stops anything, so the census never ends the
//! Nx daemon such a run is using.

use std::io;
use std::path::{Path, PathBuf};

use crate::build_volume::nx::{self, TreeHold};
use crate::error::CowshedError;
use crate::metadata::WorkspaceName;
use crate::runtime::job_groups;

/// How many holders a refusal names; the rest are counted.
const NAMED_HOLDERS: usize = 10;

/// The longest parent chain [`descends_from`] walks. Parents are older than their children, so a
/// chain ends at launchd; the bound keeps the walk total under pid reuse mid-walk.
const ANCESTRY_BOUND: usize = 1024;

/// The processes cowshed runs on a workspace, which a detach or a retirement waits for or stops
/// itself: the supervisor's process tree, the job groups its ledger records (a job that left the
/// supervisor's tree), and the checkout's recorded Nx daemon with its plugin workers. Anything
/// else holding the mount is work cowshed does not account for.
pub(crate) struct Accounted<'a> {
    /// The census of the workspace's store, which takes the walk.
    pub census: &'a crate::census::CensusScope,
    /// The process serving the workspace's supervisor, when one is known to serve.
    pub supervisor: Option<i32>,
    /// The job-group ledger beside the workspace's supervisor socket: a job that left the
    /// supervisor's tree.
    pub ledger: &'a Path,
}

/// A workspace mount that processes cowshed does not account for hold, each named with what it
/// holds. No flag overrides it: a detach or a retirement beneath them would unmount their work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountHeld {
    pub mount: PathBuf,
    pub holders: Vec<TreeHold>,
}

impl std::fmt::Display for MountHeld {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.holders.len();
        write!(
            formatter,
            "{} is held by {count} process{} cowshed does not run: ",
            self.mount.display(),
            if count == 1 { "" } else { "es" }
        )?;
        for (index, held) in self.holders.iter().take(NAMED_HOLDERS).enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{held}")?;
        }
        if count > NAMED_HOLDERS {
            write!(formatter, "; and {} more", count - NAMED_HOLDERS)?;
        }
        Ok(())
    }
}

impl MountHeld {
    /// `Conflict` naming every holder. No holder was stopped, and the hint names no override:
    /// there is none.
    pub(crate) fn refusal(&self, workspace: &WorkspaceName) -> CowshedError {
        let pids = self
            .holders
            .iter()
            .map(|held| held.holder.pid.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        CowshedError::conflict(
            format!("{workspace} is in use: {self}"),
            format!(
                "cowshed stopped none of them: let pid {pids} finish, or stop it or move its \
                 working directory off {}, then retry",
                self.mount.display()
            ),
        )
    }
}

/// The holders of `mount` that `accounted` does not cover, or `None` when only cowshed's own
/// processes hold it. One pass over the process table ([`nx::tree_holds`]), then per holder a
/// walk up its parents and one ledger read. Blocking.
pub(crate) fn unaccounted(
    mount: &Path,
    accounted: &Accounted<'_>,
) -> io::Result<Option<MountHeld>> {
    let daemon = nx::live_daemon(&mount.join(".nx/workspace-data"));
    let roots: Vec<i32> = accounted.supervisor.into_iter().chain(daemon).collect();
    let mut holders = Vec::new();
    for held in nx::tree_holds(accounted.census, mount)? {
        if descends_from(held.holder.pid, &roots) {
            continue;
        }
        if job_groups::owned_process(accounted.ledger, held.holder.pid)?.is_some() {
            continue;
        }
        holders.push(held);
    }
    Ok((!holders.is_empty()).then(|| MountHeld {
        mount: mount.to_owned(),
        holders,
    }))
}

/// Whether `pid` is one of `roots` or descends from one, by the parents the kernel records. A
/// process that exited since the census, or whose parent the kernel will not describe to this
/// user, is not shown to be cowshed's, so it stays a holder.
fn descends_from(pid: i32, roots: &[i32]) -> bool {
    let mut current = pid;
    for _ in 0..ANCESTRY_BOUND {
        if roots.contains(&current) {
            return true;
        }
        let Ok(Some(record)) = job_groups::process_record(current) else {
            return false;
        };
        match i32::try_from(record.ppid) {
            Ok(parent) if parent > 1 && parent != current => current = parent,
            _ => return false,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::fs;

    fn scratch(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-mount-activity-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::canonicalize(root).unwrap()
    }

    /// A holder's script: `sh` blocked reading its stdin, a pipe the returned child holds.
    const HOLD: &str = "read _";
    /// [`HOLD`] with the file `$0` names open as descriptor 3.
    const HOLD_OPEN: &str = "exec 3<\"$0\"; read _";

    /// `sh` with its working directory at `cwd`, holding `open` as descriptor 3 when given. It
    /// runs until the test drops the child's stdin: no clock decides how long it holds.
    fn holder(cwd: &Path, open: Option<&Path>) -> std::process::Child {
        let mut command = std::process::Command::new("/bin/sh");
        match open {
            Some(file) => command.arg("-c").arg(HOLD_OPEN).arg(file),
            None => command.arg("-c").arg(HOLD),
        };
        command
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap()
    }

    /// Asked until every child shows: `sh` opens descriptor 3 after the spawn returns. A child
    /// that exited first fails the test by name; one that never shows is a hang the leg's
    /// no-progress watchdog names, not a bound of this test's own.
    fn census_naming(
        mount: &Path,
        accounted: &Accounted<'_>,
        children: &mut [&mut std::process::Child],
    ) -> Vec<TreeHold> {
        loop {
            let held = unaccounted(mount, accounted)
                .unwrap()
                .map_or_else(Vec::new, |held| held.holders);
            if children.iter().all(|child| {
                let pid = i32::try_from(child.id()).unwrap();
                held.iter().any(|holder| holder.holder.pid == pid)
            }) {
                return held;
            }
            for child in children.iter_mut() {
                if let Some(status) = child.try_wait().unwrap() {
                    panic!(
                        "holder {} exited before it held {}: {status}",
                        child.id(),
                        mount.display()
                    );
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Host work on a mount with no supervisor: one process working in it, another with one of
    /// its files open from outside. Each is named with what it holds and where; a process working
    /// beside the mount is not named.
    #[test]
    fn a_process_working_in_a_mount_and_one_with_a_file_open_there_both_hold_it() {
        let mount = scratch("held");
        let packages = mount.join("packages");
        fs::create_dir_all(&packages).unwrap();
        let lock = mount.join("bun.lock");
        fs::write(&lock, b"lock").unwrap();
        let beside = scratch("beside");
        let ledger = beside.join("supervisor.groups");
        let census = crate::census::this_test();
        let accounted = Accounted {
            census: &census,
            supervisor: None,
            ledger: &ledger,
        };
        let mut working = holder(&packages, None);
        let mut reading = holder(&beside, Some(&lock));
        let mut elsewhere = holder(&beside, None);
        let pids = [&working, &reading, &elsewhere].map(|child| i32::try_from(child.id()).unwrap());
        let held = census_naming(&mount, &accounted, &mut [&mut working, &mut reading]);
        let named = unaccounted(&mount, &accounted).unwrap();
        for child in [&mut working, &mut reading, &mut elsewhere] {
            drop(child.stdin.take());
            child.wait_bounded().unwrap();
        }
        let hold_of = |pid: i32| {
            held.iter()
                .find(|holder| holder.holder.pid == pid)
                .map(|holder| holder.hold.clone())
        };
        assert_eq!(
            hold_of(pids[0]),
            Some(nx::Hold::WorkingDirectory(packages.clone()))
        );
        assert_eq!(hold_of(pids[1]), Some(nx::Hold::OpenFile(lock.clone())));
        assert_eq!(
            hold_of(pids[2]),
            None,
            "a process beside the mount holds nothing of it"
        );
        let said = named.expect("still held when asked again").to_string();
        for expected in [
            format!(
                "pid {} (/bin/sh -c {HOLD}), working directory {}",
                pids[0],
                packages.display()
            ),
            format!(
                "pid {} (/bin/sh -c {HOLD_OPEN} {}), open file {}",
                pids[1],
                lock.display(),
                lock.display()
            ),
        ] {
            assert!(said.contains(&expected), "{expected:?} is not in {said:?}");
        }
        fs::remove_dir_all(&mount).unwrap();
        fs::remove_dir_all(&beside).unwrap();
    }

    /// The supervisor's own processes are cowshed's to wait for or stop: a child of the
    /// supervisor working in the mount leaves it unheld.
    #[test]
    fn the_supervisors_own_children_leave_its_mount_unheld() {
        let mount = scratch("supervised");
        let beside = scratch("supervised-beside");
        let ledger = beside.join("supervisor.groups");
        let mut child = holder(&mount, None);
        let pid = i32::try_from(child.id()).unwrap();
        census_naming(
            &mount,
            &Accounted {
                census: &crate::census::this_test(),
                supervisor: None,
                ledger: &ledger,
            },
            &mut [&mut child],
        );
        let supervised = unaccounted(
            &mount,
            &Accounted {
                census: &crate::census::this_test(),
                supervisor: Some(i32::try_from(std::process::id()).unwrap()),
                ledger: &ledger,
            },
        )
        .unwrap();
        let record = job_groups::process_record(pid);
        drop(child.stdin.take());
        child.wait_bounded().unwrap();
        assert!(
            !supervised
                .iter()
                .flat_map(|held| &held.holders)
                .any(|holder| holder.holder.pid == pid),
            "the supervisor's child is named: {supervised:?}; its record {record:?}, this process {}",
            std::process::id()
        );
        fs::remove_dir_all(&mount).unwrap();
        fs::remove_dir_all(&beside).unwrap();
    }

    /// The refusal names every holder with its evidence, counts the ones past the first ten,
    /// and offers no flag that overrides it.
    #[test]
    fn a_held_mount_refusal_names_its_holders_and_no_override() {
        let mount = PathBuf::from("/mnt/raven");
        let holders = (0..12)
            .map(|pid| TreeHold {
                holder: cowshed_os::process::Holder {
                    pid: 100 + pid,
                    command: "nx run-many -t test".into(),
                },
                hold: nx::Hold::WorkingDirectory(mount.join("packages")),
            })
            .collect();
        let refusal = MountHeld {
            mount: mount.clone(),
            holders,
        }
        .refusal(&WorkspaceName::new("raven").unwrap());
        assert_eq!(refusal.code, crate::error::ErrorCode::Conflict);
        assert!(
            refusal.message.starts_with(
                "raven is in use: /mnt/raven is held by 12 processes cowshed does not run: \
                 pid 100 (nx run-many -t test), working directory /mnt/raven/packages; "
            ),
            "{}",
            refusal.message
        );
        assert!(
            refusal.message.ends_with("; and 2 more"),
            "{}",
            refusal.message
        );
        assert!(refusal.hint.contains("pid 100 101"), "{}", refusal.hint);
        assert!(
            !refusal.hint.contains("--"),
            "no override: {}",
            refusal.hint
        );
    }
}
