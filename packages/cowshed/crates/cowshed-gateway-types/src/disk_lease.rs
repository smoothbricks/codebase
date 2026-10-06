//! The host disk-lifecycle lease's wire shape, shared by the gateway that schedules it and every
//! process that runs a disk tool (specs/cowshed/05_gateway.md, "Disk-lifecycle lease").
//!
//! Every `diskutil image attach` ends in a StorageKit `syncAllDisks` call to root `storagekitd`,
//! and that sync does not finish while the host's mount table keeps changing: one loop of
//! `mount_apfs`/`umount` (6 cycles/s, no attach at all) moved a probe attach from 1.15 s to a
//! 12.2 s median, and four loops starved it for good (specs/cowshed/01_storage.md, "How the APFS
//! host degrades"). Attaches among themselves do not starve: they queue on `storagekitd`. So the
//! lease has two classes that exclude each other, and members of one class share.
//!
//! A client connects to the gateway's control socket, writes one [`DiskLeaseRequest`] line and
//! keeps the connection open without half-closing it. The gateway answers a [`LeaseState::Queued`]
//! line at once and a [`LeaseState::Granted`] line when the client may run its command, or one
//! refusal (`ok: false` with a code and an error). The lease lasts until the client closes the
//! connection, so a client that dies holding one releases it with its socket, and one that closes
//! while queued leaves the queue.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The control operation that asks for a disk lease.
pub const DISK_LEASE_OP: &str = "disk-lease";

/// Which side of the lease a disk command draws on.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiskClass {
    /// Calls that go through `storagekitd` or change the set of disks: every `diskutil` verb,
    /// `hdiutil`, `newfs_apfs`.
    Storage,
    /// Changes to the mount table: `mount_apfs` and `umount`.
    Namespace,
}

impl DiskClass {
    /// The other class, which this one never runs beside.
    pub const fn other(self) -> Self {
        match self {
            Self::Storage => Self::Namespace,
            Self::Namespace => Self::Storage,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::Namespace => "namespace",
        }
    }
}

impl fmt::Display for DiskClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The one line a client writes: `{"op":"disk-lease","class":"storage","command":"…"}`.
/// `command` names what the lease is for, so the gateway can say which command held a phase too
/// long.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct DiskLeaseRequest<'a> {
    op: &'static str,
    pub class: DiskClass,
    pub command: &'a str,
}

impl<'a> DiskLeaseRequest<'a> {
    pub const fn new(class: DiskClass, command: &'a str) -> Self {
        Self {
            op: DISK_LEASE_OP,
            class,
            command,
        }
    }
}

/// Where a lease stands, as each answer line says.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LeaseState {
    /// The gateway holds the request in its queue. Sent at once, so a client can tell a gateway
    /// that schedules leases from one that predates them, which says nothing until it reads EOF.
    Queued,
    /// The client may run its command now, and holds the lease until it closes the connection.
    Granted,
}

/// One answer line, as a client reads it. Fields the client has no use for are ignored, so the
/// gateway's other response fields never break it.
#[derive(Clone, Debug, Deserialize)]
pub struct DiskLeaseAnswer {
    pub ok: bool,
    #[serde(default)]
    pub lease: Option<LeaseState>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}
