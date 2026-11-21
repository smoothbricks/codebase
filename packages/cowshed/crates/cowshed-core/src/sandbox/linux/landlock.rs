//! Landlock as a launch uses it (04_sandbox.md, "Linux enforcement"): which kernel ABI is
//! enough, which access rights each class of rule holds at it, and the three system calls.
//!
//! A ruleset handles every filesystem right the running ABI knows, so whatever no rule grants is
//! refused, rights this code never grants included. ABI 3 is required: below it a ruleset cannot
//! decide `truncate(2)` (ABI 3) or a rename or link across directories (ABI 2), and a launch
//! never runs with less than the policy. Above it the kernel's additions are used as they
//! exist: device `ioctl(2)` (ABI 5), and the scopes that keep a job from signalling a process
//! outside its domain or connecting to an abstract Unix socket bound outside it (ABI 6).

use std::fmt;
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd};

use super::profile::Access;

/// The lowest Landlock ABI a launch runs under.
pub(super) const REQUIRED_ABI: u32 = 3;

const EXECUTE: u64 = 1 << 0;
const WRITE_FILE: u64 = 1 << 1;
const READ_FILE: u64 = 1 << 2;
const READ_DIR: u64 = 1 << 3;
const REMOVE_DIR: u64 = 1 << 4;
const REMOVE_FILE: u64 = 1 << 5;
const MAKE_CHAR: u64 = 1 << 6;
const MAKE_DIR: u64 = 1 << 7;
const MAKE_REG: u64 = 1 << 8;
const MAKE_SOCK: u64 = 1 << 9;
const MAKE_FIFO: u64 = 1 << 10;
const MAKE_BLOCK: u64 = 1 << 11;
const MAKE_SYM: u64 = 1 << 12;
const REFER: u64 = 1 << 13;
const TRUNCATE: u64 = 1 << 14;
const IOCTL_DEV: u64 = 1 << 15;

/// Every filesystem right of ABI 3.
const FILESYSTEM_ABI_3: u64 = EXECUTE
    | WRITE_FILE
    | READ_FILE
    | READ_DIR
    | REMOVE_DIR
    | REMOVE_FILE
    | MAKE_CHAR
    | MAKE_DIR
    | MAKE_REG
    | MAKE_SOCK
    | MAKE_FIFO
    | MAKE_BLOCK
    | MAKE_SYM
    | REFER
    | TRUNCATE;

/// The rights a rule on something other than a directory may hold; the kernel refuses any other
/// there.
const FILE_RIGHTS: u64 = EXECUTE | WRITE_FILE | READ_FILE | TRUNCATE | IOCTL_DEV;

const NET_BIND_TCP: u64 = 1 << 0;
const NET_CONNECT_TCP: u64 = 1 << 1;

const SCOPE_ABSTRACT_UNIX_SOCKET: u64 = 1 << 0;
const SCOPE_SIGNAL: u64 = 1 << 1;

/// `LANDLOCK_CREATE_RULESET_VERSION`.
const CREATE_RULESET_VERSION: libc::c_long = 1;
/// `LANDLOCK_RULE_PATH_BENEATH`.
const RULE_PATH_BENEATH: libc::c_long = 1;
const NO_FLAGS: libc::c_long = 0;

/// `struct landlock_ruleset_attr`. A kernel whose ABI knows fewer fields accepts the longer
/// struct while the fields it does not know are zero, which [`Abi`] guarantees.
#[repr(C)]
struct RulesetAttr {
    handled_access_fs: u64,
    handled_access_net: u64,
    scoped: u64,
}

/// `struct landlock_path_beneath_attr`, which the kernel declares packed.
#[repr(C, packed)]
struct PathBeneathAttr {
    allowed_access: u64,
    parent_fd: libc::c_int,
}

/// Why this kernel cannot hold a launch's policy.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Unsupported {
    /// The kernel has no Landlock, or it is disabled: the probe's errno.
    Absent {
        errno: i32,
    },
    Old {
        found: u32,
    },
}

impl fmt::Display for Unsupported {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent { errno } => write!(
                formatter,
                "this kernel has no Landlock ({}); cowshed requires Landlock ABI {REQUIRED_ABI} or later and runs no command unsandboxed",
                io::Error::from_raw_os_error(*errno)
            ),
            Self::Old { found } => write!(
                formatter,
                "this kernel's Landlock ABI is {found}; cowshed requires ABI {REQUIRED_ABI} or later (filesystem rules that decide rename, link and truncate) and runs no command unsandboxed"
            ),
        }
    }
}

/// A Landlock ABI that is enough for a launch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Abi(u32);

impl Abi {
    /// Whether the ABI the kernel answered (or the errno it refused the question with) is
    /// enough.
    pub(super) fn admit(probed: Result<u32, i32>) -> Result<Self, Unsupported> {
        match probed {
            Ok(found) if found >= REQUIRED_ABI => Ok(Self(found)),
            Ok(found) => Err(Unsupported::Old { found }),
            Err(errno) => Err(Unsupported::Absent { errno }),
        }
    }

    /// The running kernel's ABI, when it is enough.
    pub(super) fn probe() -> Result<Self, Unsupported> {
        // SAFETY: a null attribute of size zero with the version flag is the documented query;
        // the kernel reads no memory for it.
        let answered = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<RulesetAttr>(),
                0_usize,
                CREATE_RULESET_VERSION,
            )
        };
        Self::admit(u32::try_from(answered).map_err(|_| {
            io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::ENOSYS)
        }))
    }

    /// Every filesystem right this ABI decides: what a ruleset handles, so what no rule grants
    /// is refused.
    pub(super) fn handled_filesystem(self) -> u64 {
        if self.0 >= 5 {
            FILESYSTEM_ABI_3 | IOCTL_DEV
        } else {
            FILESYSTEM_ABI_3
        }
    }

    /// TCP bind and connect, where this ABI decides them (ABI 4).
    pub(super) fn handled_network(self) -> u64 {
        if self.0 >= 4 {
            NET_BIND_TCP | NET_CONNECT_TCP
        } else {
            0
        }
    }

    /// Signals and abstract Unix sockets confined to the domain, where this ABI scopes them
    /// (ABI 6).
    pub(super) fn scopes(self) -> u64 {
        if self.0 >= 6 {
            SCOPE_ABSTRACT_UNIX_SOCKET | SCOPE_SIGNAL
        } else {
            0
        }
    }

    /// The rights a rule of `access` holds at this ABI on a directory or on anything else.
    /// `exec` is whether reading there also starts programs. Zero means the rule has nothing to
    /// say at this ABI and is not added.
    pub(super) fn rights(self, access: Access, exec: bool, directory: bool) -> u64 {
        let read = READ_FILE | READ_DIR | if exec { EXECUTE } else { 0 };
        let wanted = match access {
            Access::Read => read,
            Access::ReadWrite => {
                read | WRITE_FILE
                    | TRUNCATE
                    | REMOVE_DIR
                    | REMOVE_FILE
                    | MAKE_REG
                    | MAKE_DIR
                    | MAKE_SYM
                    | MAKE_SOCK
                    | MAKE_FIFO
                    | REFER
            }
            Access::Files => READ_FILE | WRITE_FILE,
            Access::Device => READ_FILE | WRITE_FILE | READ_DIR | IOCTL_DEV,
        };
        let decided = wanted & self.handled_filesystem();
        if directory {
            decided
        } else {
            decided & FILE_RIGHTS
        }
    }
}

/// Whether a ruleset also closes TCP.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Network {
    /// TCP is left to the launch's network namespace.
    Unrestricted,
    /// No TCP bind or connect from here on, where the ABI decides them: the connector's, which
    /// holds its listener already.
    Closed,
}

/// A ruleset being filled, then enforced on this thread and everything it starts.
pub(super) struct Ruleset {
    descriptor: OwnedFd,
}

impl Ruleset {
    pub(super) fn create(abi: Abi, network: Network) -> io::Result<Self> {
        let attributes = RulesetAttr {
            handled_access_fs: abi.handled_filesystem(),
            handled_access_net: match network {
                Network::Unrestricted => 0,
                Network::Closed => abi.handled_network(),
            },
            scoped: abi.scopes(),
        };
        // SAFETY: `attributes` is a live `landlock_ruleset_attr` of the size passed.
        let answered = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                &raw const attributes,
                std::mem::size_of::<RulesetAttr>(),
                NO_FLAGS,
            )
        };
        let descriptor = libc::c_int::try_from(answered)
            .ok()
            .filter(|descriptor| *descriptor >= 0)
            .ok_or_else(io::Error::last_os_error)?;
        // SAFETY: the kernel just returned this descriptor, close-on-exec, to this call alone.
        Ok(Self {
            descriptor: unsafe { OwnedFd::from_raw_fd(descriptor) },
        })
    }

    /// Grant `rights` on the file `beneath` names and, when it is a directory, on everything
    /// beneath it. No rights is no rule.
    pub(super) fn allow(&self, beneath: BorrowedFd<'_>, rights: u64) -> io::Result<()> {
        if rights == 0 {
            return Ok(());
        }
        let attributes = PathBeneathAttr {
            allowed_access: rights,
            parent_fd: beneath.as_raw_fd(),
        };
        // SAFETY: `attributes` is a live `landlock_path_beneath_attr` and both descriptors are
        // open for the call.
        let answered = unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                libc::c_long::from(self.descriptor.as_raw_fd()),
                RULE_PATH_BENEATH,
                &raw const attributes,
                NO_FLAGS,
            )
        };
        if answered == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Enforce the ruleset on this thread from now on, across every exec. `no_new_privs` goes
    /// with it: the kernel requires it of an unprivileged caller, and a set-user-ID program
    /// would otherwise leave the policy.
    pub(super) fn restrict_self(self) -> io::Result<()> {
        let (set, unused): (libc::c_ulong, libc::c_ulong) = (1, 0);
        // SAFETY: PR_SET_NO_NEW_PRIVS takes integers only.
        if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, set, unused, unused, unused) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the ruleset descriptor is open for the call.
        let answered = unsafe {
            libc::syscall(
                libc::SYS_landlock_restrict_self,
                libc::c_long::from(self.descriptor.as_raw_fd()),
                NO_FLAGS,
            )
        };
        if answered == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn abi(version: u32) -> Abi {
        Abi::admit(Ok(version)).unwrap()
    }

    #[test]
    fn a_kernel_below_abi_3_or_without_landlock_is_refused_and_says_why() {
        for (probed, expected) in [
            (Ok(1), Err(Unsupported::Old { found: 1 })),
            (Ok(2), Err(Unsupported::Old { found: 2 })),
            (Ok(3), Ok(Abi(3))),
            (Ok(7), Ok(Abi(7))),
            (
                Err(libc::ENOSYS),
                Err(Unsupported::Absent {
                    errno: libc::ENOSYS,
                }),
            ),
            (
                Err(libc::EOPNOTSUPP),
                Err(Unsupported::Absent {
                    errno: libc::EOPNOTSUPP,
                }),
            ),
        ] {
            assert_eq!(Abi::admit(probed), expected, "{probed:?}");
        }
        let old = Unsupported::Old { found: 2 }.to_string();
        assert!(old.contains("ABI is 2"), "{old}");
        assert!(old.contains("requires ABI 3"), "{old}");
        assert!(old.contains("runs no command unsandboxed"), "{old}");
        let absent = Unsupported::Absent {
            errno: libc::EOPNOTSUPP,
        }
        .to_string();
        assert!(absent.contains("has no Landlock"), "{absent}");
        assert!(absent.contains("runs no command unsandboxed"), "{absent}");
    }

    #[test]
    fn a_ruleset_handles_exactly_what_its_abi_decides() {
        for (version, ioctl, network, scopes) in [
            (3, false, 0, 0),
            (4, false, NET_BIND_TCP | NET_CONNECT_TCP, 0),
            (5, true, NET_BIND_TCP | NET_CONNECT_TCP, 0),
            (
                6,
                true,
                NET_BIND_TCP | NET_CONNECT_TCP,
                SCOPE_ABSTRACT_UNIX_SOCKET | SCOPE_SIGNAL,
            ),
            (
                7,
                true,
                NET_BIND_TCP | NET_CONNECT_TCP,
                SCOPE_ABSTRACT_UNIX_SOCKET | SCOPE_SIGNAL,
            ),
        ] {
            let abi = abi(version);
            assert_eq!(
                abi.handled_filesystem() & FILESYSTEM_ABI_3,
                FILESYSTEM_ABI_3
            );
            assert_eq!(
                abi.handled_filesystem() & IOCTL_DEV != 0,
                ioctl,
                "{version}"
            );
            assert_eq!(
                abi.handled_filesystem() >> 16,
                0,
                "no right this code does not name"
            );
            assert_eq!(abi.handled_network(), network, "{version}");
            assert_eq!(abi.scopes(), scopes, "{version}");
        }
    }

    #[test]
    fn each_access_holds_its_rights_and_a_file_only_those_a_file_may() {
        let write = WRITE_FILE
            | TRUNCATE
            | REMOVE_DIR
            | REMOVE_FILE
            | MAKE_REG
            | MAKE_DIR
            | MAKE_SYM
            | MAKE_SOCK
            | MAKE_FIFO
            | REFER;
        let abi = abi(7);
        for (access, exec, directory, expected) in [
            (Access::Read, true, true, READ_FILE | READ_DIR | EXECUTE),
            (Access::Read, false, true, READ_FILE | READ_DIR),
            (Access::Read, true, false, READ_FILE | EXECUTE),
            (Access::Read, false, false, READ_FILE),
            (
                Access::ReadWrite,
                true,
                true,
                READ_FILE | READ_DIR | EXECUTE | write,
            ),
            (
                Access::ReadWrite,
                false,
                false,
                READ_FILE | WRITE_FILE | TRUNCATE,
            ),
            (Access::Files, true, true, READ_FILE | WRITE_FILE),
            (Access::Files, true, false, READ_FILE | WRITE_FILE),
            (
                Access::Device,
                true,
                true,
                READ_FILE | WRITE_FILE | READ_DIR | IOCTL_DEV,
            ),
            (
                Access::Device,
                true,
                false,
                READ_FILE | WRITE_FILE | IOCTL_DEV,
            ),
        ] {
            assert_eq!(
                abi.rights(access, exec, directory),
                expected,
                "{access:?} exec={exec} directory={directory}"
            );
        }
        for access in [
            Access::Read,
            Access::ReadWrite,
            Access::Files,
            Access::Device,
        ] {
            let rights = abi.rights(access, true, true);
            assert_eq!(
                rights & (MAKE_CHAR | MAKE_BLOCK),
                0,
                "{access:?} makes no device node"
            );
        }
    }

    #[test]
    fn a_right_an_older_abi_does_not_decide_is_never_asked_of_it() {
        assert_eq!(
            abi(4).rights(Access::Device, false, false),
            READ_FILE | WRITE_FILE,
            "device ioctl is not a right before ABI 5"
        );
        assert_ne!(abi(3).rights(Access::ReadWrite, true, true) & REFER, 0);
        assert_ne!(abi(3).rights(Access::ReadWrite, true, true) & TRUNCATE, 0);
    }
}
