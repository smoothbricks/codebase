//! The macOS mechanism of [`super`]: `proc_pidinfo` identity reads, `proc_listchildpids`, and
//! kqueue `EVFILT_PROC` registrations whose cookie is the life's unique id (`EV_UDATA_SPECIFIC`,
//! so two lives of one pid are two registrations and each event names its own).

use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::time::{Duration, Instant};

use super::{Delivered, ExitGrant, Identity, Life, ProcessEvent};

const WATCHED: u32 = libc::NOTE_FORK | libc::NOTE_EXEC | libc::NOTE_EXIT | libc::NOTE_EXITSTATUS;
/// What the kernel grants for a life it refuses `NOTE_EXITSTATUS` with `EACCES`.
const WATCHED_WITHOUT_STATUS: u32 = libc::NOTE_FORK | libc::NOTE_EXEC | libc::NOTE_EXIT;
/// The events one wait takes from the kernel at most; the rest wait for the next.
const BATCH: usize = 64;

/// `struct proc_uniqidentifierinfo` (`<sys/proc_info.h>`), which the `libc` crate does not
/// declare: `p_uuid[16]`, `p_uniqueid`, `p_puniqueid`, `p_idversion`, then 20 reserved bytes.
#[repr(C, align(8))]
struct UniqueIdentifierInfo {
    _uuid: [u8; 16],
    unique_id: u64,
    parent_unique_id: u64,
    id_version: i32,
    _reserved: [u8; 20],
}

/// `PROC_PIDUNIQIDENTIFIERINFO`, which the kernel answers for a process of any effective uid.
const PROC_PIDUNIQIDENTIFIERINFO: libc::c_int = 17;

/// The identity of the life `pid` holds now; `None` only when no running life holds it (`ESRCH`:
/// exited, reaped or not). Every other failure is an error.
pub fn identity(pid: u32) -> io::Result<Option<Identity>> {
    let raw = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    let mut info = std::mem::MaybeUninit::<UniqueIdentifierInfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<UniqueIdentifierInfo>())
        .map_err(io::Error::other)?;
    // SAFETY: flavor 17 writes at most `size` bytes of struct proc_uniqidentifierinfo into the
    // writable, zeroed buffer.
    let written = unsafe {
        libc::proc_pidinfo(
            raw,
            PROC_PIDUNIQIDENTIFIERINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        if written <= 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::ESRCH) => Ok(None),
                _ => Err(error),
            };
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "proc_pidinfo flavor {PROC_PIDUNIQIDENTIFIERINFO} for process {pid} returned {written} bytes, expected {size}"
            ),
        ));
    }
    // SAFETY: the kernel filled the whole payload, whose integer and byte-array fields admit
    // every bit pattern.
    let info = unsafe { info.assume_init() };
    Ok(Some(Identity {
        life: Life {
            pid,
            birth: info.unique_id,
        },
        parent_birth: info.parent_unique_id,
        version: info.id_version.cast_unsigned(),
    }))
}

/// The pids whose parent is `parent`'s pid, listed while that pid held `parent` before and after
/// the list: `None` once it does not, so a later process given the pid never lends its children.
/// A pid listed is a child of that life when read; its own life is adopted only when its
/// `parent_birth` is `parent`'s. libproc answers the kernel's -1 with 0 and leaves errno set, so
/// a 0 is an empty list only when errno stayed clear across the call.
pub fn children(parent: Life) -> io::Result<Option<Vec<u32>>> {
    let holds = |identity: Option<Identity>| identity.is_some_and(|now| now.life == parent);
    if !holds(identity(parent.pid)?) {
        return Ok(None);
    }
    let raw = libc::pid_t::try_from(parent.pid).map_err(io::Error::other)?;
    let mut capacity = 64_usize;
    let listed = loop {
        let mut pids: Vec<libc::pid_t> = vec![0; capacity];
        let bytes = libc::c_int::try_from(capacity * std::mem::size_of::<libc::pid_t>())
            .map_err(io::Error::other)?;
        // SAFETY: `__error` names this thread's errno.
        unsafe { *libc::__error() = 0 };
        // SAFETY: `pids` is writable for `bytes` bytes.
        let count = unsafe { libc::proc_listchildpids(raw, pids.as_mut_ptr().cast(), bytes) };
        let error = io::Error::last_os_error();
        let count = match (usize::try_from(count), error.raw_os_error()) {
            (Ok(count), Some(0)) => count,
            (Ok(count), _) if count > 0 => count,
            (_, Some(0)) => {
                return Err(io::Error::other(format!(
                    "proc_listchildpids for process {} returned {count} without an errno",
                    parent.pid
                )));
            }
            _ => return Err(error),
        };
        if count < capacity {
            pids.truncate(count);
            break pids;
        }
        capacity *= 2;
    };
    if !holds(identity(parent.pid)?) {
        return Ok(None);
    }
    Ok(Some(
        listed
            .into_iter()
            .filter_map(|child| u32::try_from(child).ok())
            .filter(|&child| child > 0)
            .collect(),
    ))
}

/// One kqueue of exact lives. A kqueue is not inherited across `fork` (kqueue(2)), so no child
/// spawned meanwhile keeps it.
#[derive(Debug)]
pub struct ProcessWatch(OwnedFd);

impl ProcessWatch {
    pub fn new() -> io::Result<Self> {
        // SAFETY: kqueue takes nothing and returns a new descriptor.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh descriptor this watch alone owns.
        Ok(Self(unsafe { OwnedFd::from_raw_fd(queue) }))
    }

    /// Watch `life`'s fork, exec and exit, with its exit status where the kernel grants it, and
    /// answer the grant. The watch is armed on the pid, so it is `life`'s only if the pid still
    /// holds `life` once armed: the life is read again, and a watch that landed on another
    /// process is withdrawn. `None` when `life` no longer runs.
    pub fn register(&self, life: Life) -> io::Result<Option<ExitGrant>> {
        let add = libc::EV_ADD | libc::EV_CLEAR;
        let grant = match self.change(life, add, WATCHED) {
            Ok(()) => ExitGrant::Status,
            Err(refused) if refused.raw_os_error() == Some(libc::EACCES) => {
                match self.change(life, add, WATCHED_WITHOUT_STATUS) {
                    Ok(()) => ExitGrant::Withheld,
                    Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
                    Err(error) => return Err(error),
                }
            }
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
            Err(error) => return Err(error),
        };
        if identity(life.pid)?.is_some_and(|now| now.life == life) {
            return Ok(Some(grant));
        }
        self.withdraw(life)?;
        Ok(None)
    }

    /// Withdraw `life`'s watch; `false` when no registration or process remains for it.
    pub fn withdraw(&self, life: Life) -> io::Result<bool> {
        match self.change(life, libc::EV_DELETE, WATCHED) {
            Ok(()) => Ok(true),
            Err(error) if matches!(error.raw_os_error(), Some(libc::ESRCH | libc::ENOENT)) => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Wait up to `within` (`None`: until one comes) and append every event the kernel hands over
    /// to `into`: answers how many, zero only when `within` passed. An interrupted wait resumes
    /// within what is left of `within`.
    pub fn events(&self, within: Option<Duration>, into: &mut Vec<Delivered>) -> io::Result<usize> {
        let until = within.map(|within| Instant::now() + within);
        // SAFETY: a zeroed kevent is plain data the kernel may overwrite.
        let mut batch = [unsafe { std::mem::zeroed::<libc::kevent>() }; BATCH];
        let capacity = libc::c_int::try_from(BATCH).map_err(io::Error::other)?;
        let ready = loop {
            let timeout = until
                .map(|until| {
                    let left = until.saturating_duration_since(Instant::now());
                    Ok::<_, io::Error>(libc::timespec {
                        tv_sec: libc::time_t::try_from(left.as_secs()).map_err(io::Error::other)?,
                        tv_nsec: libc::c_long::from(left.subsec_nanos()),
                    })
                })
                .transpose()?;
            // SAFETY: `batch` is writable for `capacity` events; no changes are passed; the
            // timeout, when given, is live for the call.
            let ready = unsafe {
                libc::kevent(
                    self.0.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    batch.as_mut_ptr(),
                    capacity,
                    timeout
                        .as_ref()
                        .map_or(std::ptr::null(), std::ptr::from_ref),
                )
            };
            match usize::try_from(ready) {
                Ok(ready) => break ready,
                Err(_) => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
            }
        };
        into.extend(batch[..ready].iter().map(delivered));
        Ok(ready)
    }

    /// Apply one change to `life`'s registration, keyed by its unique id: the kernel's answer.
    fn change(&self, life: Life, flags: u16, fflags: u32) -> io::Result<()> {
        let change = libc::kevent {
            ident: usize::try_from(life.pid).map_err(io::Error::other)?,
            filter: libc::EVFILT_PROC,
            flags: flags | libc::EV_UDATA_SPECIFIC,
            fflags,
            data: 0,
            udata: std::ptr::without_provenance_mut(
                usize::try_from(life.birth).map_err(io::Error::other)?,
            ),
        };
        // SAFETY: one change read from live memory, no event list, on the owned queue. With no
        // room for events a refused change is the call's own error.
        let changed = unsafe {
            libc::kevent(
                self.0.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if changed == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

/// One kernel event as delivered: `struct kevent` is packed on Darwin, so each field is copied
/// out before it is used.
fn delivered(event: &libc::kevent) -> Delivered {
    let (ident, filter, flags, fflags, data, udata) = (
        event.ident,
        event.filter,
        event.flags,
        event.fflags,
        event.data,
        event.udata,
    );
    let refused = |error| Delivered::Refused { ident, error };
    let invalid = |what: String| refused(io::Error::new(io::ErrorKind::InvalidData, what));
    if flags & libc::EV_ERROR != 0 {
        return match i32::try_from(data) {
            Ok(errno) => refused(io::Error::from_raw_os_error(errno)),
            Err(_) => invalid(format!("process event {ident} refused with errno {data}")),
        };
    }
    if filter != libc::EVFILT_PROC {
        return invalid(format!("process event {ident} has filter {filter}"));
    }
    let Ok(pid) = u32::try_from(ident) else {
        return invalid(format!("process event ident {ident} is no pid"));
    };
    let Ok(birth) = u64::try_from(udata.addr()) else {
        return invalid(format!("process {pid}'s event cookie is no unique id"));
    };
    let Ok(datum) = libc::c_int::try_from(data) else {
        return invalid(format!(
            "process {pid}'s event datum {data} is no wait status"
        ));
    };
    Delivered::Event(ProcessEvent {
        life: Life { pid, birth },
        forked: fflags & libc::NOTE_FORK != 0,
        execed: fflags & libc::NOTE_EXEC != 0,
        exited: fflags & libc::NOTE_EXIT != 0,
        datum,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fork_lock::Spawn as _;
    use crate::process_events::Exit;
    use crate::test_backstop::{ChildBackstop as _, NO_PROGRESS};
    use std::io::{BufRead as _, Write as _};

    /// A kevent as the kernel would deliver it for `pid`'s registration armed for `birth`.
    fn kevent(
        pid: usize,
        filter: i16,
        flags: u16,
        fflags: u32,
        data: isize,
        birth: usize,
    ) -> libc::kevent {
        libc::kevent {
            ident: pid,
            filter,
            flags,
            fflags,
            data,
            udata: std::ptr::without_provenance_mut(birth),
        }
    }

    /// Each delivered event names its life by the registration's cookie and carries every
    /// coalesced bit; a refusal, a foreign filter and a datum no wait status fits are refused,
    /// never read as an event.
    #[test]
    fn a_kernel_event_decodes_to_its_life_and_every_coalesced_bit() {
        let all = libc::NOTE_FORK | libc::NOTE_EXEC | libc::NOTE_EXIT;
        let life = Life { pid: 42, birth: 7 };
        for (event, expected) in [
            (
                kevent(42, libc::EVFILT_PROC, 0, all, 0x300, 7),
                Some((true, true, true, Some(Exit::Status(0x300)))),
            ),
            (
                kevent(42, libc::EVFILT_PROC, 0, libc::NOTE_EXEC, 0, 7),
                Some((false, true, false, None)),
            ),
            (
                kevent(
                    42,
                    libc::EVFILT_PROC,
                    libc::EV_ERROR,
                    0,
                    isize::try_from(libc::ESRCH).expect("an errno fits isize"),
                    7,
                ),
                None,
            ),
            (kevent(42, libc::EVFILT_READ, 0, all, 0, 7), None),
            (kevent(42, libc::EVFILT_PROC, 0, all, isize::MAX, 7), None),
        ] {
            match (delivered(&event), expected) {
                (Delivered::Event(decoded), Some((forked, execed, exited, exit))) => {
                    assert_eq!(decoded.life, life);
                    assert_eq!(
                        (
                            decoded.forked,
                            decoded.execed,
                            decoded.exited,
                            decoded.exit(ExitGrant::Status)
                        ),
                        (forked, execed, exited, exit)
                    );
                }
                (Delivered::Refused { ident, .. }, None) => assert_eq!(ident, 42),
                (delivered, expected) => panic!("{delivered:?}, expected {expected:?}"),
            }
        }
    }

    /// The root this test owns, with the input its held child reads: on every exit path the input
    /// closes, which ends that child, and the root is killed if it still runs, then reaped.
    struct Owned {
        root: std::process::Child,
        input: Option<std::process::ChildStdin>,
    }

    impl Drop for Owned {
        fn drop(&mut self) {
            drop(self.input.take());
            match self.root.try_wait() {
                Ok(Some(_)) => {}
                Ok(None) => {
                    if let Err(error) = self.root.kill() {
                        crate::eprintln!("kill the owned root: {error}");
                    }
                }
                Err(error) => crate::eprintln!("read the owned root's state: {error}"),
            }
            if let Err(error) = self.root.wait_bounded() {
                crate::eprintln!("reap the owned root: {error}");
            }
        }
    }

    /// The root announces readiness and holds before its fork. Its child then announces
    /// readiness and holds before exec. Each exact life is armed before its gate opens,
    /// so fork, exec and both exits are actual kernel events, not scheduling guesses.
    /// A reaped life and a life whose pid has another owner are never armed.
    #[test]
    fn a_watched_life_reports_its_fork_exec_and_exit_status() {
        let root = std::process::Command::new("sh")
            .args([
                "-c",
                "echo root-ready; read fork; (echo child-ready; read go; exec cat); exit 3",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .expect("spawn sh");
        let mut owned = Owned { root, input: None };
        owned.input = owned.root.stdin.take();
        let mut output =
            std::io::BufReader::new(owned.root.stdout.take().expect("the root's stdout"));
        let mut ready = String::new();
        output
            .read_line(&mut ready)
            .expect("read the root's readiness");
        assert_eq!(
            ready, "root-ready\n",
            "the root ran before its life is read"
        );
        let root = identity(owned.root.id())
            .expect("read the root's identity")
            .expect("the root waits on its fork gate")
            .life;
        let watch = ProcessWatch::new().expect("a process watch");
        assert_eq!(
            watch.register(root).expect("arm the root"),
            Some(ExitGrant::Status)
        );
        owned
            .input
            .as_mut()
            .expect("the root's input")
            .write_all(b"fork\n")
            .expect("release the fork gate");
        ready.clear();
        output
            .read_line(&mut ready)
            .expect("read the child's readiness");
        assert_eq!(
            ready, "child-ready\n",
            "the child ran before its life is read"
        );
        let child = children(root)
            .expect("list the root's children")
            .expect("the root still runs")
            .into_iter()
            .filter_map(|pid| identity(pid).expect("read a child's identity"))
            .filter(|child| child.parent_birth == root.birth)
            .map(|child| child.life)
            .collect::<Vec<_>>();
        let [child] = child[..] else {
            panic!("the root's one held child, not {child:?}");
        };
        let grant = watch
            .register(child)
            .expect("arm the child")
            .expect("the child holds on its input");
        let mut delivered = Vec::new();
        let next = |delivered: &mut Vec<Delivered>| {
            let got = watch
                .events(Some(NO_PROGRESS), delivered)
                .expect("read the process events");
            assert!(
                got > 0,
                "no process event within the backstop: {delivered:?}"
            );
        };
        let event = |delivered: &[Delivered], life: Life, what: fn(&ProcessEvent) -> bool| {
            delivered.iter().any(|delivered| {
                matches!(delivered, Delivered::Event(event) if event.life == life && what(event))
            })
        };
        while !event(&delivered, root, |event| event.forked) {
            next(&mut delivered);
        }
        let input = owned.input.as_mut().expect("the child's input");
        input.write_all(b"go\n").expect("release the child");
        while !event(&delivered, child, |event| event.execed) {
            next(&mut delivered);
        }
        drop(owned.input.take());
        while !event(&delivered, child, |event| event.exited)
            || !event(&delivered, root, |event| event.exited)
        {
            next(&mut delivered);
        }
        let exit = |life: Life, grant: ExitGrant| {
            delivered.iter().find_map(|delivered| match delivered {
                Delivered::Event(event) if event.life == life => event.exit(grant),
                Delivered::Event(_) | Delivered::Refused { .. } => None,
            })
        };
        assert!(exit(child, grant).is_some(), "{delivered:?}");
        assert!(
            matches!(exit(root, ExitGrant::Status), Some(Exit::Status(status)) if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 3),
            "{delivered:?}"
        );
        assert!(
            !delivered
                .iter()
                .any(|delivered| matches!(delivered, Delivered::Refused { .. })),
            "{delivered:?}"
        );
        assert_eq!(
            owned.root.wait_bounded().expect("reap the root").code(),
            Some(3)
        );
        assert_eq!(watch.register(root).expect("arm a reaped life"), None);
        let this = identity(std::process::id())
            .expect("read this process's identity")
            .expect("this process runs")
            .life;
        let stranger = Life {
            pid: this.pid,
            birth: this.birth ^ 1,
        };
        assert_eq!(
            watch
                .register(stranger)
                .expect("arm a life the pid does not hold"),
            None
        );
    }
}
