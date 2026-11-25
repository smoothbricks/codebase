//! The process table as Linux publishes it: procfs. Every census reads `/proc/<pid>` and nothing
//! else, so a process of another user, or one that exited since the listing, shows nothing it
//! holds. An open file is named by its device and inode, read by `stat` through the process's own
//! link to it, so no path a process was started with or renamed since decides what it holds.

use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use cowshed_os::process::Holder;

use super::{Hold, TreeHold, command_line, linux_process_is_sandboxed, listed, walk_begins};
use crate::census::{Asked, CensusScope, Lineage};

/// Whether `pid` runs inside cowshed's Linux sandbox: either in a different user or network
/// namespace from this controller (`sandbox/linux/launcher.rs` unshares both) or inside a job
/// cgroup (`runtime/job_cgroup.rs`). A host daemon under a `NoNewPrivileges` systemd service
/// shares both namespaces and sits in no job cgroup, so it is recognized as a host daemon.
/// An exited or unreadable (`ENOENT`/`ESRCH`/`EACCES`) process fails closed as `true`.
pub(super) fn daemon_is_sandboxed(pid: libc::pid_t) -> io::Result<bool> {
    let own_user = fs::read_link("/proc/self/ns/user")?;
    let own_net = fs::read_link("/proc/self/ns/net")?;
    let read_ns = |kind: &str| -> io::Result<Option<std::ffi::OsString>> {
        let path = format!("/proc/{pid}/ns/{kind}");
        match fs::read_link(&path) {
            Ok(target) => Ok(Some(target.into_os_string())),
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    || error.kind() == io::ErrorKind::PermissionDenied
                    || error.raw_os_error() == Some(libc::ESRCH) =>
            {
                Ok(None)
            }
            Err(error) => Err(io::Error::new(error.kind(), format!("{path}: {error}"))),
        }
    };
    let process_user = read_ns("user")?;
    let process_net = read_ns("net")?;
    let in_job_cgroup = match crate::runtime::job_cgroup::job_cgroup_of(pid) {
        Ok(cgroup) => Some(cgroup.is_some()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    Ok(linux_process_is_sandboxed(
        own_user.as_os_str(),
        own_net.as_os_str(),
        process_user.as_deref(),
        process_net.as_deref(),
        in_job_cgroup,
    ))
}

/// The processes with a descriptor open on each of `files`, in order, from one pass over the
/// processes a census asks about ([`census_pids`]): each descriptor's device and inode
/// ([`descriptor_identities`]) against each file's. A missing file has no holder. No mapped region
/// is walked: a database or lock is only ever held through a descriptor. Linux has no event-only
/// open to leave out: an inotify watch holds no descriptor on the file it watches.
pub(super) fn descriptor_holders(
    census: &CensusScope,
    files: &[PathBuf],
) -> io::Result<Vec<Vec<Holder>>> {
    let mut identities = Vec::with_capacity(files.len());
    for file in files {
        identities.push(match fs::metadata(file) {
            Ok(metadata) => Some((metadata.dev(), metadata.ino())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        });
    }
    let mut held = vec![Vec::new(); files.len()];
    if identities.iter().all(Option::is_none) {
        return Ok(held);
    }
    for pid in census_pids(census)?.pids {
        let mut holds = vec![false; files.len()];
        for (_, identity) in descriptor_identities(pid) {
            for (hold, file) in holds.iter_mut().zip(&identities) {
                *hold |= *file == Some(identity);
            }
        }
        if holds.contains(&true) {
            let command = command_line(pid);
            for (holders, _) in held.iter_mut().zip(holds).filter(|(_, hold)| *hold) {
                holders.push(Holder {
                    pid,
                    command: command.clone(),
                });
            }
        }
    }
    Ok(held)
}

/// Every process with a descriptor on the inode `(device, inode)`, with those descriptors: each
/// process [`census_pids`] asks about, each descriptor's device and inode
/// ([`descriptor_identities`]).
pub(crate) fn inode_holders(
    census: &CensusScope,
    identity: (u64, u64),
) -> io::Result<Vec<(Holder, Vec<i32>)>> {
    let mut holders = Vec::new();
    for pid in census_pids(census)?.pids {
        let descriptors: Vec<i32> = descriptor_identities(pid)
            .into_iter()
            .filter(|(_, file)| *file == identity)
            .map(|(descriptor, _)| descriptor)
            .collect();
        if !descriptors.is_empty() {
            holders.push((
                Holder {
                    pid,
                    command: command_line(pid),
                },
                descriptors,
            ));
        }
    }
    Ok(holders)
}

/// Each descriptor of `pid` with its file's device and inode: one `/proc/<pid>/fd` listing, then
/// one `stat` through each entry, which reaches the open file itself whatever its path names now.
/// A process that exited or is withheld from this user has none, and a descriptor closed since the
/// listing names none.
fn descriptor_identities(pid: libc::pid_t) -> Vec<(i32, (u64, u64))> {
    let Ok(descriptors) = fs::read_dir(format!("/proc/{pid}/fd")) else {
        return Vec::new();
    };
    descriptors
        .flatten()
        .filter_map(|descriptor| {
            let number = descriptor.file_name().to_str()?.parse().ok()?;
            let file = fs::metadata(descriptor.path()).ok()?;
            Some((number, (file.dev(), file.ino())))
        })
        .collect()
}

/// Every process holding anything on the volume mounted at `mount` -- its working or root
/// directory, its executable, an open file, a thread's working or root directory, or another
/// mapped file -- which is what keeps `umount` answering `EBUSY`. By device ([`on_volume`]), over
/// the processes a census asks about ([`census_pids`]), so a holder is named whatever path it
/// reached the volume by, another mount namespace's included. A missing mount has none.
pub fn volume_holders(census: &CensusScope, mount: &Path) -> io::Result<Vec<Holder>> {
    let device = match fs::metadata(mount) {
        Ok(metadata) => metadata.dev(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut holders = Vec::new();
    for pid in census_pids(census)?.pids {
        if on_volume(pid, device) {
            holders.push(Holder {
                pid,
                command: command_line(pid),
            });
        }
    }
    Ok(holders)
}

/// Whether `pid` holds anything on the volume of `device`. Cheapest first: its working directory,
/// root directory and executable (one `stat` each through `/proc/<pid>/{cwd,root,exe}`), its
/// descriptors ([`descriptor_identities`]), the working and root directory of each thread
/// (`/proc/<pid>/task/<tid>/{cwd,root}`: a thread that unshared its filesystem state has its own),
/// then its mapped files ([`maps_on`]). A process that exited or is withheld from this user holds
/// none.
fn on_volume(pid: libc::pid_t, device: u64) -> bool {
    let process = PathBuf::from(format!("/proc/{pid}"));
    let on = |link: PathBuf| fs::metadata(link).is_ok_and(|file| file.dev() == device);
    if ["cwd", "root", "exe"]
        .into_iter()
        .any(|name| on(process.join(name)))
    {
        return true;
    }
    if descriptor_identities(pid)
        .into_iter()
        .any(|(_, (on_device, _))| on_device == device)
    {
        return true;
    }
    if fs::read_dir(process.join("task")).is_ok_and(|threads| {
        threads.flatten().any(|thread| {
            ["cwd", "root"]
                .into_iter()
                .any(|name| on(thread.path().join(name)))
        })
    }) {
        return true;
    }
    maps_on(&process, device)
}

/// Whether the process at `/proc/<pid>` maps a file on `device`: the fourth field of a
/// `/proc/<pid>/maps` line is the mapped file's device as `major:minor` in hex and the fifth its
/// inode, which is 0 for a mapping of no file. A filesystem that answers `stat` with one device
/// per subvolume and `maps` with another (btrfs) shows no mapping here; its holder is still named
/// by its directories and descriptors.
fn maps_on(process: &Path, device: u64) -> bool {
    let (major, minor) = (libc::major(device), libc::minor(device));
    fs::read_to_string(process.join("maps")).is_ok_and(|maps| {
        maps.lines().any(|mapping| {
            let mut fields = mapping.split_whitespace().skip(3);
            let (Some(mapped), Some(inode)) = (fields.next(), fields.next()) else {
                return false;
            };
            inode != "0"
                && mapped
                    .split_once(':')
                    .is_some_and(|(mapped_major, mapped_minor)| {
                        u32::from_str_radix(mapped_major, 16) == Ok(major)
                            && u32::from_str_radix(mapped_minor, 16) == Ok(minor)
                    })
        })
    })
}

/// Every process holding anything inside the directory tree at `root` -- its working directory,
/// its root, its executable, an open file, a mapped file, checked in that order -- whether or not
/// `root` is a volume's mount point, each with the first hold found. A plain checkout's holders,
/// which `volume_holders` cannot name without naming its whole volume. By path, as procfs names
/// each hold, so a process in another mount namespace is named only when its path there is the
/// same. `root` is canonicalized first, so a symlinked path names the tree its target does; a
/// missing `root` has none.
pub fn tree_holds(census: &CensusScope, root: &Path) -> io::Result<Vec<TreeHold>> {
    let root = match fs::canonicalize(root) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let under = |link: PathBuf| link.starts_with(&root).then_some(link);
    let mut holds = Vec::new();
    for pid in census_pids(census)?.pids {
        let process = PathBuf::from(format!("/proc/{pid}"));
        let dirs = procinfo::directories(pid).ok().flatten();
        let hold = dirs
            .as_ref()
            .and_then(|dirs| dirs.cwd.as_ref().filter(|path| path.starts_with(&root)))
            .cloned()
            .map(Hold::WorkingDirectory)
            .or_else(|| {
                dirs.and_then(|dirs| dirs.root)
                    .filter(|path| path.starts_with(&root))
                    .map(Hold::RootDirectory)
            })
            .or_else(|| executable(pid).and_then(under).map(Hold::Executable))
            .or_else(|| {
                procinfo::open_paths(pid)
                    .ok()?
                    .ok()?
                    .into_iter()
                    .find_map(|entry| under(entry.path))
                    .map(Hold::OpenFile)
            })
            // A mapping's path is its line's last field and the line's first `/`: the
            // address range, permissions, offset, device and inode before it hold none.
            .or_else(|| {
                fs::read_to_string(process.join("maps"))
                    .ok()?
                    .lines()
                    .find_map(|mapping| under(PathBuf::from(&mapping[mapping.find('/')?..])))
                    .map(Hold::MappedFile)
            });
        if let Some(hold) = hold {
            holds.push(TreeHold {
                holder: Holder {
                    pid,
                    command: command_line(pid),
                },
                hold,
            });
        }
    }
    Ok(holds)
}

/// The processes `census` asks about ([`crate::census::asked`]): of every live pid, those its reach
/// covers. Linux records no unique id, so no lease passes a process over: its census asks every
/// process in its reach. The one walker every Linux census starts from ([`walk_begins`]).
pub(super) fn census_pids(census: &CensusScope) -> io::Result<Asked> {
    Ok(crate::census::asked(
        census.reach(),
        &listed(census)?,
        &std::collections::BTreeSet::new(),
    ))
}

/// `pid`'s parent and process group, the two fields after the state in `/proc/<pid>/stat` (the
/// command before them, in parentheses, may hold any byte but the last `)`). `None` once it has
/// exited.
pub(super) fn lineage_of(pid: libc::pid_t) -> Option<Lineage> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let mut fields = stat[stat.rfind(')')? + 1..].split_whitespace().skip(1);
    Some(Lineage {
        parent: fields.next()?.parse().ok()?,
        group: fields.next()?.parse().ok()?,
    })
}

/// Every pid procfs lists now: the one walker every Linux census starts from, counted once per
/// census ([`walk_begins`]).
pub(super) fn all_pids() -> io::Result<Vec<libc::pid_t>> {
    walk_begins();
    let mut pids = Vec::new();
    for process in fs::read_dir("/proc")? {
        if let Some(pid) = process?
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        {
            pids.push(pid);
        }
    }
    Ok(pids)
}

/// `pid`'s executable path (`/proc/<pid>/exe`), or `None` if the kernel names none: a kernel
/// thread, a process that exited, or one withheld from this user.
pub(super) fn executable(pid: libc::pid_t) -> Option<PathBuf> {
    fs::read_link(format!("/proc/{pid}/exe")).ok()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::MetadataExt as _;

    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;

    use super::super::tests::{holder_ready, scratch};
    use super::{descriptor_identities, inode_holders, maps_on, on_volume, volume_holders};

    /// A descriptor names its file by device and inode, never by the path it was opened by: a
    /// database renamed after a process opened it still names that process, under its new path and
    /// through a hard link, and a file put where it was names none.
    #[test]
    fn a_descriptor_holds_its_file_whatever_path_names_it_now() {
        let root = scratch("linux-identity");
        let opened = root.join("opened.db");
        fs::write(&opened, b"").unwrap();
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 3<\"$0\"; echo ready; read _")
            .arg(&opened)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = i32::try_from(child.id()).unwrap();
        holder_ready(&mut child);
        let renamed = root.join("renamed.db");
        fs::rename(&opened, &renamed).unwrap();
        let linked = root.join("linked.db");
        fs::hard_link(&renamed, &linked).unwrap();
        fs::write(&opened, b"").unwrap();
        let census = crate::census::this_test();
        let pids = |path| -> Vec<i32> {
            super::super::holders(&census, path)
                .unwrap()
                .iter()
                .map(|holder| holder.pid)
                .collect()
        };
        let (by_rename, by_link, replaced) = (pids(&renamed), pids(&linked), pids(&opened));
        let file = fs::metadata(&renamed).unwrap();
        let by_inode = inode_holders(&census, (file.dev(), file.ino())).unwrap();
        drop(child.stdin.take());
        child.wait_bounded().unwrap();
        assert_eq!(by_rename, [pid]);
        assert_eq!(by_link, [pid]);
        assert_eq!(
            replaced,
            Vec::<i32>::new(),
            "the file put at the opened path is another"
        );
        assert_eq!(
            by_inode
                .iter()
                .map(|(holder, descriptors)| (holder.pid, descriptors.as_slice()))
                .collect::<Vec<_>>(),
            [(pid, [3].as_slice())]
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// This process is on its own executable's volume and on its working directory's, and maps its
    /// executable; no process is on a device no filesystem has.
    #[test]
    fn a_process_is_on_the_volume_of_what_it_runs_and_maps() {
        let own = i32::try_from(std::process::id()).unwrap();
        let executable = fs::metadata("/proc/self/exe").unwrap().dev();
        let working = fs::metadata("/proc/self/cwd").unwrap().dev();
        assert!(on_volume(own, executable));
        assert!(on_volume(own, working));
        assert!(!on_volume(own, 0), "no file is on device 0:0");
        assert!(!maps_on(std::path::Path::new("/proc/self"), 0));
        assert!(
            maps_on(std::path::Path::new("/proc/self"), executable),
            "this process maps its executable"
        );
        assert!(
            !descriptor_identities(own).is_empty(),
            "this process's own descriptors are listed"
        );
        let census = crate::census::this_test();
        let on_executable = volume_holders(&census, std::path::Path::new("/proc/self/exe"))
            .unwrap()
            .iter()
            .any(|holder| holder.pid == own);
        assert!(
            on_executable,
            "this process runs from its executable's volume"
        );
    }
}
