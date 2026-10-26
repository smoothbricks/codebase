//! Exact lives and the kernel's events for them: a fork, an exec, an exit.
//!
//! A pid names a process only while it runs; the kernel gives the same number to a later one. A
//! [`Life`] is therefore a pid with the kernel's unique id for the process holding it, and every
//! read here is fenced to that life: an identity is read before and after the read it fences,
//! and a pid that holds another life by then is answered as gone, never as that life. A watch is
//! armed on the pid the kernel addresses, so [`ProcessWatch::register`] rereads the life after
//! arming and withdraws a watch that landed on a later process. A failed read is an error,
//! never an empty or absent answer; only the kernel's "no such process" is absence.
//!
//! The interface is platform-free; each platform's mechanism lives in its own file behind it.

use std::io;

/// One life: a pid and the kernel's unique id for the process that holds it (Darwin's
/// `p_uniqueid`). The id is an identity, compared for equality only: it is never a time, an order
/// or a count, and no other process is given it while the system runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Life {
    pub pid: u32,
    pub birth: u64,
}

/// The identity the kernel reports for the life a pid holds now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Identity {
    pub life: Life,
    /// The unique id of the life's parent: a child is adopted only when this is its parent's.
    pub parent_birth: u64,
    /// The pid version, which an exec renews while the life stays the same.
    pub version: u32,
}

/// What the kernel granted a registered life's exit: its wait status, or the exit alone. Darwin
/// grants the status only for this process's child or a process it may SIGKILL, and the exit
/// alone for every process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExitGrant {
    Status,
    Withheld,
}

/// A life's exit as its registration's grant reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exit {
    /// The life's wait status.
    Status(libc::c_int),
    /// The exit, its status withheld from this watcher.
    Withheld,
}

/// What one event says of its life. The kernel coalesces a life's events, so one event can carry
/// a fork, an exec and an exit together; each is set as it happened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessEvent {
    /// The life the event's registration was armed for, never the pid alone.
    pub life: Life,
    pub forked: bool,
    pub execed: bool,
    pub exited: bool,
    /// The kernel's datum: the wait status when the exit's status was granted.
    datum: libc::c_int,
}

impl ProcessEvent {
    /// The life's exit, as `grant`, the registration's own, reports it; `None` when it did not exit.
    pub fn exit(&self, grant: ExitGrant) -> Option<Exit> {
        self.exited.then_some(match grant {
            ExitGrant::Status => Exit::Status(self.datum),
            ExitGrant::Withheld => Exit::Withheld,
        })
    }
}

/// One delivered event: its life's event, or the kernel's refusal for the registration armed on
/// `ident`, the pid the kernel names. Every event of a batch is delivered, a refusal beside the
/// others.
#[derive(Debug)]
pub enum Delivered {
    Event(ProcessEvent),
    Refused { ident: usize, error: io::Error },
}

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::{ProcessWatch, children, identity};
