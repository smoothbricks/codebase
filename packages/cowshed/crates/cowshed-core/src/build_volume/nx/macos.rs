//! The process table as Darwin publishes it: libproc (`proc_listallpids`, `proc_pidinfo`,
//! `proc_pidfdinfo`, `proc_pidpath`). An open file is named by its vnode's device and inode, and
//! an event-only open (`O_EVTONLY`) holds nothing.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use cowshed_os::process::Holder;

use super::{Hold, TreeHold, command_line, listed, walk_begins};
use crate::census::{Asked, CensusScope, Lineage};

/// Whether `pid` runs inside a sandbox: Darwin's `sandbox_check` (`procinfo::sandboxed`).
pub(super) fn daemon_is_sandboxed(pid: libc::pid_t) -> io::Result<bool> {
    procinfo::sandboxed(pid)
}

/// The processes with a descriptor open on each of `files`, in order, from one pass over the
/// processes a census asks about ([`census_pids`]): each vnode descriptor's device and inode
/// (`PROC_PIDFDVNODEINFO`) against each file's. Event-only opens hold nothing. A missing file has
/// no holder. No mapped region is walked: a database or lock is only ever held through a
/// descriptor.
pub(super) fn descriptor_holders(
    census: &CensusScope,
    files: &[PathBuf],
) -> io::Result<Vec<Vec<Holder>>> {
    use std::os::unix::fs::MetadataExt;
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
    let vinfo_size = c_size::<VnodeFdinfo>()?;
    for pid in census_pids(census)?.pids {
        let mut holds = vec![false; files.len()];
        for fd in vnode_descriptors(pid)? {
            let Some(identity) = vnode_fd_identity(pid, fd, vinfo_size) else {
                continue;
            };
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

/// Every process with a vnode descriptor on the inode `(device, inode)`, with those descriptors:
/// each process [`census_pids`] asks about, each vnode descriptor's device and inode
/// (`PROC_PIDFDVNODEINFO`). Event-only opens hold nothing. A process a cowshed lifecycle operation
/// runs in is not asked: a fork's first mount of its new image holds that image's vnode for up to
/// minutes, and every per-descriptor query of the fork waits that long (measured 2026-10-25: 245
/// extent-map copies of 50-221 s, the `image-lock wait` holder line and `lsof` stalled behind them).
/// A lock's custody takes such a holder from the lock's own record instead
/// (`held_lock::kernel_view`).
pub(crate) fn inode_holders(
    census: &CensusScope,
    identity: (u64, u64),
) -> io::Result<Vec<(Holder, Vec<i32>)>> {
    let vinfo_size = c_size::<VnodeFdinfo>()?;
    let mut holders = Vec::new();
    for pid in census_pids(census)?.pids {
        let descriptors: Vec<i32> = vnode_descriptors(pid)?
            .into_iter()
            .filter(|&fd| vnode_fd_identity(pid, fd, vinfo_size) == Some(identity))
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

/// Every process holding anything on the volume mounted at `mount` -- its working or root
/// directory, a thread's working directory, an open file, its executable or another mapped file --
/// which is what keeps `umount` answering `Resource busy`. Event-only opens (FSEvents, Spotlight)
/// never block an unmount and are left out. It is the walk libproc's `proc_listpidspath` makes
/// with `PROC_LISTPIDSPATH_PATH_IS_VOLUME | PROC_LISTPIDSPATH_EXCLUDE_EVTONLY`, by device, over the
/// processes a census asks about ([`census_pids`]) rather than libproc's own listing, which asks
/// every process, a lifecycle operation's included ([`on_volume`]). A missing mount has none.
pub fn volume_holders(census: &CensusScope, mount: &Path) -> io::Result<Vec<Holder>> {
    use std::os::unix::fs::MetadataExt;
    let device = match fs::metadata(mount) {
        Ok(metadata) => metadata.dev(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut holders = Vec::new();
    for pid in census_pids(census)?.pids {
        if on_volume(pid, device)? {
            holders.push(Holder {
                pid,
                command: command_line(pid),
            });
        }
    }
    Ok(holders)
}

/// Whether `pid` holds anything on the volume of `device`, asked as `proc_listpidspath` asks it
/// (xnu `libsyscall/wrappers/libproc/proc_listpidspath.c`): its working and root directory
/// (`PROC_PIDVNODEPATHINFO`), its vnode descriptors ([`vnode_fd_identity`]), the working directory
/// of each thread that has its own (only for a process the kernel flags `PROC_FLAG_THCWD`), then
/// the first mapped region on the device, its executable's text included (one call,
/// `PROC_PIDREGIONPATHINFO3`). A process that exited or is withheld from this user holds none.
fn on_volume(pid: libc::pid_t, device: u64) -> io::Result<bool> {
    let on = |stat: &libc::vinfo_stat| u64::from(stat.vst_dev) == device;
    let size = c_size::<libc::proc_vnodepathinfo>()?;
    // SAFETY: `proc_vnodepathinfo` is plain C data, for which all zeros is a valid value; it is
    // read only when the kernel fills all of it.
    let mut directories: libc::proc_vnodepathinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `directories` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            std::ptr::from_mut(&mut directories).cast(),
            size,
        )
    };
    if filled == size
        && (on(&directories.pvi_cdir.vip_vi.vi_stat) || on(&directories.pvi_rdir.vip_vi.vi_stat))
    {
        return Ok(true);
    }
    let vinfo_size = c_size::<VnodeFdinfo>()?;
    if vnode_descriptors(pid)?
        .into_iter()
        .any(|fd| vnode_fd_identity(pid, fd, vinfo_size).is_some_and(|(dev, _)| dev == device))
    {
        return Ok(true);
    }
    if thread_directories(pid)?.iter().any(on) {
        return Ok(true);
    }
    // The private flavor `proc_listpidspath` asks for the first region whose file is on the device
    // its argument names (xnu `bsd/sys/proc_info_private.h`).
    const PROC_PIDREGIONPATHINFO3: libc::c_int = 23;
    let size = c_size::<RegionWithPath>()?;
    // SAFETY: `RegionWithPath` is plain C data, for which all zeros is a valid value; it is read
    // only when the kernel fills all of it.
    let mut region: RegionWithPath = unsafe { std::mem::zeroed() };
    // SAFETY: `region` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            PROC_PIDREGIONPATHINFO3,
            device,
            std::ptr::from_mut(&mut region).cast(),
            size,
        )
    };
    Ok(filled == size && on(&region.prp_vip.vip_vi.vi_stat))
}

/// `sys/proc_info.h`'s `struct proc_regionwithpathinfo`, which libc 0.2 does not bind: the 96-byte
/// `proc_regioninfo` (four `uint32_t`, a `uint64_t`, fourteen `uint32_t`, two `uint64_t`), then the
/// region's file.
#[repr(C)]
struct RegionWithPath {
    _prp_prinfo: [u64; 12],
    prp_vip: libc::vnode_info_path,
}

/// The attributes of each thread working directory `pid`'s threads set of their own
/// (`pthread_chdir_np`): none unless the kernel flags the process `PROC_FLAG_THCWD`, then one
/// listing (`PROC_PIDLISTTHREADS`) and one call per thread (`PROC_PIDTHREADPATHINFO`).
fn thread_directories(pid: libc::pid_t) -> io::Result<Vec<libc::vinfo_stat>> {
    // `sys/proc_info.h`; libc 0.2 binds none of the three.
    const PROC_FLAG_THCWD: u32 = 0x100;
    const PROC_PIDLISTTHREADS: libc::c_int = 6;
    const PROC_PIDTHREADPATHINFO: libc::c_int = 10;
    let size = c_size::<libc::proc_bsdinfo>()?;
    // SAFETY: `proc_bsdinfo` is plain C data, for which all zeros is a valid value; it is read only
    // when the kernel fills all of it.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if filled != size || info.pbi_flags & PROC_FLAG_THCWD == 0 {
        return Ok(Vec::new());
    }
    let mut capacity = 64usize;
    let threads = loop {
        let mut threads = vec![0u64; capacity];
        let bytes = libc::c_int::try_from(capacity * size_of::<u64>()).map_err(io::Error::other)?;
        // SAFETY: `threads` is writable for `bytes` bytes, its whole length.
        let filled = unsafe {
            libc::proc_pidinfo(
                pid,
                PROC_PIDLISTTHREADS,
                0,
                threads.as_mut_ptr().cast(),
                bytes,
            )
        };
        let Ok(filled) = usize::try_from(filled) else {
            return Ok(Vec::new());
        };
        let count = filled / size_of::<u64>();
        // A listing that filled the buffer may have been cut short: grow it and list again.
        if count >= capacity {
            capacity *= 4;
            continue;
        }
        threads.truncate(count);
        break threads;
    };
    let size = c_size::<ThreadWithPath>()?;
    let mut directories = Vec::new();
    for thread in threads {
        // SAFETY: `ThreadWithPath` is plain C data, for which all zeros is a valid value; it is
        // read only when the kernel fills all of it.
        let mut info: ThreadWithPath = unsafe { std::mem::zeroed() };
        // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
        let filled = unsafe {
            libc::proc_pidinfo(
                pid,
                PROC_PIDTHREADPATHINFO,
                thread,
                std::ptr::from_mut(&mut info).cast(),
                size,
            )
        };
        if filled == size {
            directories.push(info.pvip.vip_vi.vi_stat);
        }
    }
    Ok(directories)
}

/// `sys/proc_info.h`'s `struct proc_threadwithpathinfo`, which libc 0.2 does not bind: a thread's
/// `proc_threadinfo`, then its own working directory.
#[repr(C)]
struct ThreadWithPath {
    _pt: libc::proc_threadinfo,
    pvip: libc::vnode_info_path,
}

/// Every process holding anything inside the directory tree at `root` -- its working directory
/// or root directory, its executable, an open file -- whether or not `root` is a volume's mount
/// point, each with the first hold found ([`hold_in_tree`]). A plain checkout's holders, which
/// `volume_holders` cannot name without naming its whole volume. A process the kernel withholds
/// from this user, or that exited since the listing, shows none. A missing `root` has none.
pub fn tree_holds(census: &CensusScope, root: &Path) -> io::Result<Vec<TreeHold>> {
    let root = match fs::canonicalize(root) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut holds = Vec::new();
    for pid in census_pids(census)?.pids {
        if let Some(hold) = hold_in_tree(pid, &root)? {
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

/// What `pid` holds inside `root`, on `device`, if anything. Cheapest first: the directories
/// cost one call, the executable one more, and the descriptors a listing plus one call per vnode.
fn hold_in_tree(pid: libc::pid_t, root: &Path) -> io::Result<Option<Hold>> {
    if let Some(hold) = directories(pid)?
        .into_iter()
        .find(|hold| hold.path().starts_with(root))
    {
        return Ok(Some(hold));
    }
    if let Some(executable) = executable(pid).filter(|path| path.starts_with(root)) {
        return Ok(Some(Hold::Executable(executable)));
    }
    Ok(open_vnodes(pid)?
        .into_iter()
        .find(|path| path.starts_with(root))
        .map(Hold::OpenFile))
}

/// `pid`'s working directory and root directory (`procinfo::directories`). A process the kernel
/// withholds from this user, or that exited, names none.
fn directories(pid: libc::pid_t) -> io::Result<Vec<Hold>> {
    let dirs = match procinfo::directories(pid) {
        Ok(Some(dirs)) => dirs,
        Ok(None) => return Ok(Vec::new()),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    Ok([
        dirs.cwd.map(Hold::WorkingDirectory),
        dirs.root.map(Hold::RootDirectory),
    ]
    .into_iter()
    .flatten()
    .collect())
}

/// The path of each open vnode descriptor of `pid` (`procinfo::open_paths`). A descriptor closed
/// since the listing names none, and an event-only open (`O_EVTONLY`: FSEvents, Spotlight, a file
/// watcher) holds nothing, as `volume_holders` leaves it out.
fn open_vnodes(pid: libc::pid_t) -> io::Result<Vec<PathBuf>> {
    let Ok(paths) = procinfo::open_paths(pid)? else {
        return Ok(Vec::new());
    };
    Ok(paths
        .into_iter()
        .filter(|entry| !entry.event_only)
        .map(|entry| entry.path)
        .collect())
}

/// `pid`'s vnode descriptors: one listing (`PROC_PIDLISTFDS`). A process that exited or is
/// withheld from this user has none.
fn vnode_descriptors(pid: libc::pid_t) -> io::Result<Vec<libc::c_int>> {
    // Descriptors opened between the sizing call and the listing still fit.
    const HEADROOM: usize = 32;
    let entry = size_of::<libc::proc_fdinfo>();
    // SAFETY: a null buffer of zero bytes asks only how many bytes the listing needs.
    let needed =
        unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
    // Negative: the process exited or is withheld from this user, so it holds no descriptor here.
    let Ok(needed) = usize::try_from(needed) else {
        return Ok(Vec::new());
    };
    let mut capacity = needed / entry + HEADROOM;
    loop {
        let mut fds = vec![
            libc::proc_fdinfo {
                proc_fd: 0,
                proc_fdtype: 0
            };
            capacity
        ];
        let bytes = libc::c_int::try_from(capacity * entry).map_err(io::Error::other)?;
        // SAFETY: `fds` is writable for `bytes` bytes, its whole length.
        let filled = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDLISTFDS,
                0,
                fds.as_mut_ptr().cast(),
                bytes,
            )
        };
        let Ok(filled) = usize::try_from(filled) else {
            return Ok(Vec::new());
        };
        let count = filled / entry;
        // A listing that filled the buffer may have been cut short: grow it and list again.
        if count >= capacity {
            capacity *= 4;
            continue;
        }
        fds.truncate(count);
        return Ok(fds
            .into_iter()
            .filter(|fd| {
                libc::c_int::try_from(fd.proc_fdtype)
                    .is_ok_and(|kind| kind == libc::PROX_FDTYPE_VNODE)
            })
            .map(|fd| fd.proc_fd)
            .collect());
    }
}

/// The device and inode of vnode descriptor `fd` of `pid` (`PROC_PIDFDVNODEINFO`), or `None` if
/// the descriptor closed since its listing or is an event-only open. `size` is `VnodeFdinfo`'s
/// size: the kernel fills exactly that or nothing.
fn vnode_fd_identity(pid: libc::pid_t, fd: libc::c_int, size: libc::c_int) -> Option<(u64, u64)> {
    // libc 0.2 binds neither this flavor nor `vnode_fdinfo`; `sys/proc_info.h` defines the
    // flavor as 1.
    const PROC_PIDFDVNODEINFO: libc::c_int = 1;
    let mut info = VnodeFdinfo {
        pfi_openflags: 0,
        _pfi_rest: [0; 20],
        // SAFETY: `vnode_info` is plain C data (integers), for which all zeros is a valid value;
        // it is read only when the kernel fills all of it, as checked below.
        pvi: unsafe { std::mem::zeroed() },
    };
    // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidfdinfo(
            pid,
            fd,
            PROC_PIDFDVNODEINFO,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if filled != size || info.pfi_openflags & O_EVTONLY != 0 {
        return None;
    }
    Some((
        u64::from(info.pvi.vi_stat.vst_dev),
        info.pvi.vi_stat.vst_ino,
    ))
}

/// `sys/proc_info.h`'s `struct vnode_fdinfo`, which libc 0.2 does not bind: the 24-byte
/// `proc_fileinfo` head whose first field is the descriptor's open flags, then the vnode's
/// attributes.
#[repr(C)]
struct VnodeFdinfo {
    pfi_openflags: u32,
    _pfi_rest: [u8; 20],
    pvi: libc::vnode_info,
}

/// `fcntl.h`'s `O_EVTONLY` as `proc_fileinfo.fi_openflags` carries it: an open for event
/// notification only, which holds no data and never blocks an unmount.
const O_EVTONLY: u32 = 0x0000_8000;

/// The processes `census` asks about ([`crate::census::asked`]): of every live pid, those its reach
/// covers (all of them for the host store, the namespace owner's tree for a fixture's), but those a
/// cowshed lifecycle operation of its store runs in, as their leases record them
/// ([`crate::lease_holder::running_lifecycle`]). Such a process can hold one of its descriptors'
/// vnodes in one kernel call for a minute -- a fork's read-write mount of a new clone, which pays
/// the clone's extent-map copy -- and every per-descriptor query of it waits that long; the
/// operation's own leases and image locks already refuse what a census guards. The one walker
/// every macOS census starts from, counted once per census ([`walk_begins`]).
pub(super) fn census_pids(census: &CensusScope) -> io::Result<Asked> {
    census_pids_reading(census, |lease| fs::read(lease))
}

/// [`census_pids`], reading each lease of the census's store with `read`.
fn census_pids_reading(
    census: &CensusScope,
    read: impl FnMut(&Path) -> io::Result<Vec<u8>>,
) -> io::Result<Asked> {
    let lifecycle = crate::lease_holder::running_lifecycle(census.store(), read);
    Ok(crate::census::asked(
        census.reach(),
        &listed(census)?,
        &lifecycle,
    ))
}

/// `pid`'s parent and process group (`PROC_PIDT_SHORTBSDINFO`): one call that reads the
/// process's own fields and never a vnode, so it never waits on a kernel call the process is in.
/// `None` once it has exited, or when the kernel will not describe it to this user: such a process
/// is in no fixture's reach.
pub(super) fn lineage_of(pid: libc::pid_t) -> Option<Lineage> {
    let size = c_size::<libc::proc_bsdshortinfo>().ok()?;
    // SAFETY: `proc_bsdshortinfo` is plain C data, for which all zeros is a valid value; it is
    // read only when the kernel fills all of it.
    let mut info: libc::proc_bsdshortinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDT_SHORTBSDINFO,
            0,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if filled != size {
        return None;
    }
    Some(Lineage {
        parent: libc::pid_t::try_from(info.pbsi_ppid).ok()?,
        group: libc::pid_t::try_from(info.pbsi_pgid).ok()?,
    })
}

/// Every live pid the kernel lists (`proc_listallpids`, which answers a count of pids, not bytes),
/// except pid 0. The listing grows until it fits.
pub(super) fn all_pids() -> io::Result<Vec<libc::pid_t>> {
    walk_begins();
    let mut capacity = 256usize;
    loop {
        let mut pids: Vec<libc::pid_t> = vec![0; capacity];
        let bytes =
            libc::c_int::try_from(capacity * size_of::<libc::pid_t>()).map_err(io::Error::other)?;
        // SAFETY: `pids` is writable for `bytes` bytes, its whole length.
        let filled = unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), bytes) };
        if filled < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(filled).map_err(io::Error::other)?;
        if count < capacity {
            pids.truncate(count);
            return Ok(pids.into_iter().filter(|&pid| pid > 0).collect());
        }
        capacity *= 4;
    }
}

/// `T`'s byte size as the `c_int` the kernel's buffer arguments take.
fn c_size<T>() -> io::Result<libc::c_int> {
    libc::c_int::try_from(size_of::<T>()).map_err(io::Error::other)
}

/// The path a NUL-terminated `c_char` buffer holds, or `None` if it holds none. `c_char` is signed
/// on x86-64 Apple and unsigned on arm64 Apple; `to_ne_bytes` reads either as the same byte.
fn c_path(bytes: &[libc::c_char]) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    let path: Vec<u8> = bytes
        .iter()
        .take_while(|&&byte| byte != 0)
        .map(|&byte| u8::from_ne_bytes(byte.to_ne_bytes()))
        .collect();
    if path.is_empty() {
        return None;
    }
    Some(PathBuf::from(std::ffi::OsString::from_vec(path)))
}

/// `pid`'s executable path (`proc_pidpath`), or `None` if the kernel names none.
pub(super) fn executable(pid: libc::pid_t) -> Option<PathBuf> {
    let mut path: Vec<libc::c_char> =
        vec![0; usize::try_from(libc::PROC_PIDPATHINFO_MAXSIZE).ok()?];
    let capacity = u32::try_from(path.len()).ok()?;
    // SAFETY: `path` is writable for `capacity` bytes, its whole length.
    let length = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), capacity) };
    // A negative length is the kernel naming nothing: the conversion fails and `None` follows.
    path.truncate(usize::try_from(length).ok()?);
    c_path(&path)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::process::Holder;
    use cowshed_os::test_backstop::ChildBackstop as _;

    use super::super::tests::{holder_ready, scratch};
    use super::super::{holders, tree_holders};
    use super::{all_pids, census_pids, census_pids_reading, volume_holders};

    /// A census asks nothing of a process a held lifecycle lease names with the unique id it has
    /// now: [`census_pids`], the one walker, passes over it, so the descriptor, tree and volume
    /// censuses never query a descriptor of it (a fork in its new build image's first write, which
    /// its read-write mount makes, held one for 54-90 s). A foreign holder of the same file is
    /// still found by each census, this process is asked whatever its lease says, and a record
    /// whose unique id another life has names nothing, so a reused pid is asked again.
    #[test]
    fn a_census_asks_nothing_of_a_process_a_held_lifecycle_lease_names() {
        let root = scratch("lifecycle-lease");
        let sessions = root.join("store/acme/widget/sessions");
        fs::create_dir_all(&sessions).unwrap();
        crate::storage::bootstrap::write_owner_marker(&root.join("store/acme")).unwrap();
        let database = root.join("T-v3.db");
        fs::write(&database, b"").unwrap();
        let hold = || {
            std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg("exec 3<\"$0\"; echo ready; read _")
                .arg(&database)
                .current_dir(&root)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn_locked()
                .unwrap()
        };
        let (mut lifecycle, mut foreign) = (hold(), hold());
        let (lifecycle_pid, foreign_pid) = (lifecycle.id() as i32, foreign.id() as i32);
        let own_pid = std::process::id() as i32;
        let _own = fs::File::open(&database).unwrap();
        let pids = |holders: &[Holder]| holders.iter().map(|holder| holder.pid).collect::<Vec<_>>();
        holder_ready(&mut lifecycle);
        holder_ready(&mut foreign);
        let life = crate::runtime::job_groups::process_record(lifecycle_pid)
            .unwrap()
            .expect("the lifecycle stand-in runs")
            .unique_id;
        let record = |path: PathBuf, process: crate::lease_holder::Process| {
            fs::write(path, serde_json::to_vec(&process).unwrap()).unwrap();
        };
        let fork = |unique_id| crate::lease_holder::Process {
            pid: lifecycle.id(),
            unique_id: Some(crate::api::held_lock::DecimalU64::new(unique_id)),
            since: "2026-10-21T00:00:00Z".to_owned(),
            command: "cowshed fork main topic".to_owned(),
        };
        record(sessions.join("topic.intent.lock"), fork(life));
        record(
            sessions.join("main.land.lock"),
            crate::lease_holder::Process::this().unwrap(),
        );
        let census = crate::storage::bootstrap::CanonicalRoots::fixture(
            cowshed_os::namespace::TestNamespace::of(std::process::id(), 0).unwrap(),
            root.join("store"),
            crate::calm_pressure::calm(),
        )
        .census();
        let censuses = || {
            (
                census_pids(&census).unwrap().pids,
                pids(&holders(&census, &database).unwrap()),
                pids(&tree_holders(&census, &root).unwrap()),
                pids(&volume_holders(&census, &root).unwrap()),
            )
        };
        let (asked, held, tree, volume) = censuses();
        assert!(
            !asked.contains(&lifecycle_pid),
            "the lifecycle process was asked"
        );
        assert!(asked.contains(&foreign_pid) && asked.contains(&own_pid));
        for (census, found) in [("descriptor", &held), ("tree", &tree), ("volume", &volume)] {
            assert!(
                found.contains(&foreign_pid) && !found.contains(&lifecycle_pid),
                "{census} census: {found:?}"
            );
        }
        assert!(held.contains(&own_pid), "this process is asked: {held:?}");

        record(sessions.join("topic.intent.lock"), fork(life + 1));
        let (asked, held, _, _) = censuses();
        assert!(asked.contains(&lifecycle_pid) && held.contains(&lifecycle_pid));

        for child in [&mut lifecycle, &mut foreign] {
            drop(child.stdin.take());
            child.wait_bounded().unwrap();
        }
        fs::remove_dir_all(&root).unwrap();
    }

    /// A census reads the lifecycle leases of the store its roots name and no other store's: a
    /// fixture's census opens each lease of the fixture store once, and none of another store
    /// beside it nor of the host's (02_workspaces.md, "Holders cowshed does not run"). The lease
    /// reader is the only injected part; the store comes from the fixture's roots.
    #[test]
    fn a_fixture_census_reads_its_own_stores_leases_and_no_other() {
        let root = scratch("census-leases");
        let lease = |store: &Path, project: &str, name: &str| {
            let sessions = store
                .join("acme")
                .join(project)
                .join(crate::repository::SESSIONS_DIRECTORY);
            fs::create_dir_all(&sessions).unwrap();
            crate::storage::bootstrap::write_owner_marker(&store.join("acme")).unwrap();
            let path = sessions.join(name);
            let holder = crate::lease_holder::Process::this().unwrap();
            fs::write(&path, serde_json::to_vec(&holder).unwrap()).unwrap();
            path
        };
        let (fixture_store, other_store) = (root.join("fixture"), root.join("other"));
        let mut expected = vec![
            lease(&fixture_store, "widget", "main.land.lock"),
            lease(&fixture_store, "widget", "topic.intent.lock"),
            lease(&fixture_store, "gadget", "main.intent.lock"),
        ];
        lease(&other_store, "widget", "main.land.lock");
        let census = crate::storage::bootstrap::CanonicalRoots::fixture(
            cowshed_os::namespace::TestNamespace::of(std::process::id(), 0).unwrap(),
            fixture_store,
            crate::calm_pressure::calm(),
        )
        .census();
        let mut opened = Vec::new();
        let asked = census_pids_reading(&census, |lease| {
            opened.push(lease.to_owned());
            fs::read(lease)
        })
        .unwrap();
        opened.sort();
        expected.sort();
        assert_eq!(
            opened, expected,
            "the census read the fixture store's leases alone"
        );
        assert!(
            asked.pids.contains(&(std::process::id() as i32)),
            "a lease naming this process passes nothing over"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// A fixture's census asks the process that minted its namespace, the processes in its process
    /// group and their descendants, and no other. A run the fixture backgrounded from a shell that
    /// exited, which launchd adopted, stays in the group and is named; a process launchd adopted
    /// after it made a session of its own, as a detached daemon does, is never asked, so no
    /// per-descriptor query of a process outside the fixture can stall it.
    #[test]
    fn a_fixture_census_asks_its_owners_group_and_tree_alone() {
        let root = scratch("census-reach");
        let database = root.join("T-v3.db");
        fs::write(&database, b"").unwrap();
        // The shell starts both holders in the background, prints their pids and exits, so launchd
        // adopts them. Each reads this test's pipe, kept on descriptor 4, so it ends when the pipe
        // closes, on every exit path of this test.
        let mut detaching = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(concat!(
                "exec 4<&0; ",
                "(exec 3<\"$0\"; echo ready; read _ <&4) & echo \"grouped $!\"; ",
                "/usr/bin/perl -MPOSIX -e 'POSIX::setsid() > 0 or die \"setsid: $!\"; ",
                "open(my $held, \"<\", $ARGV[0]) or die \"open: $!\"; $| = 1; ",
                "print \"ready\\n\"; <STDIN>' \"$0\" <&4 & echo \"own $!\"",
            ))
            .arg(&database)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let mut lines =
            std::io::BufRead::lines(std::io::BufReader::new(detaching.stdout.take().unwrap()));
        let (mut grouped, mut own) = (None, None);
        for _ in 0..4 {
            let line = lines.next().unwrap().unwrap();
            match line.split_once(' ') {
                Some(("grouped", pid)) => grouped = Some(pid.parse::<i32>().unwrap()),
                Some(("own", pid)) => own = Some(pid.parse::<i32>().unwrap()),
                _ => assert_eq!(line, "ready"),
            }
        }
        let (grouped, own) = (grouped.unwrap(), own.unwrap());
        let holding = detaching.stdin.take();
        assert!(detaching.wait_bounded().unwrap().success());
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 3<\"$0\"; echo ready; read _")
            .arg(&database)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        holder_ready(&mut child);
        let child_pid = child.id() as i32;
        let census = crate::census::this_test();
        let asked = census_pids(&census).unwrap().pids;
        let mut held: Vec<i32> = holders(&census, &database)
            .unwrap()
            .iter()
            .map(|holder| holder.pid)
            .collect();
        held.sort_unstable();
        let parents = [grouped, own].map(|pid| {
            crate::runtime::job_groups::process_record(pid)
                .unwrap()
                .map(|record| record.ppid)
        });
        let listed = all_pids().unwrap();
        drop(child.stdin.take());
        child.wait_bounded().unwrap();
        drop(holding);
        assert_eq!(parents, [Some(1), Some(1)], "launchd adopted both holders");
        assert!(
            listed.contains(&own),
            "the holder with a session of its own runs"
        );
        assert!(
            asked.contains(&grouped) && asked.contains(&child_pid) && !asked.contains(&own),
            "asked {asked:?}: grouped {grouped}, own session {own}, child {child_pid}"
        );
        let mut expected = vec![grouped, child_pid];
        expected.sort_unstable();
        assert_eq!(
            held, expected,
            "the group's and the tree's holders are named"
        );
        fs::remove_dir_all(&root).unwrap();
    }
}
