//! The RAM disks the gateway attaches and detaches (specs/cowshed/05_gateway.md, "RAM disks").
//!
//! `hdiutil attach -nomount ram://<n>` forks the `diskimages-helper` the disk lives in. Run from
//! the gateway, which no job contains, the helper is no job's member, so no job's retirement ends
//! the disk under another job's use of it. The attach runs through [`SystemCommandRunner`] like
//! every disk child, as a protected disk-image command that is never killed. The detach is the
//! runner's registry-bound `hdiutil detach` ([`SystemCommandRunner::run_detach_of_entry`]): the
//! entry is read again inside the lease, immediately before the spawn.

use std::path::PathBuf;

use cowshed_gateway_types::{
    DiskClass, RamAttachRefusal, RamDetachStatus, RamDetached, RamDiskAttached, RamDiskDevice,
    RamDiskEntry, RamDiskSectors, RamDisks,
};

use crate::disk_lease::{lease_socket, leased_at};
use crate::{
    CommandRequest, CommandRunFailure, DISK_CHILD_DEADLINE, HDIUTIL, ProcessStatus,
    SystemCommandRunner,
};
use cowshed_os::namespace::HostNamespace;

/// Attaches and detaches RAM disks under a storage lease from the gateway that serves them, the
/// one of their namespace ([`lease_socket`]): production's gateway for production, a test
/// namespace's fixture gateway for its own, so a fixture's disks never ask production's.
#[derive(Clone, Debug)]
pub struct SystemRamDisks {
    runner: SystemCommandRunner,
    lease_socket: PathBuf,
}

impl SystemRamDisks {
    pub fn new(namespace: HostNamespace) -> Self {
        Self {
            runner: SystemCommandRunner::new(namespace),
            lease_socket: lease_socket(namespace),
        }
    }
}

impl RamDisks for SystemRamDisks {
    fn attach(&self, sectors: RamDiskSectors) -> Result<RamDiskAttached, RamAttachRefusal> {
        let memory = host_memory_bytes().map_err(|error| {
            RamAttachRefusal::Failed(format!(
                "cannot read the host's memory size (hw.memsize): {error}"
            ))
        })?;
        if !sectors.bytes().is_some_and(|bytes| bytes <= memory) {
            return Err(RamAttachRefusal::TooLarge { sectors, memory });
        }
        let image = format!("ram://{}", sectors.get());
        let command = format!("hdiutil attach -nomount {image}");
        let request = CommandRequest::new(HDIUTIL, ["attach", "-nomount", image.as_str()]);
        let output = self
            .runner
            .run_within(&request, DISK_CHILD_DEADLINE, |deadline, run| {
                leased_at(
                    &self.lease_socket,
                    DiskClass::Storage,
                    &command,
                    deadline,
                    run,
                )
            })
            .map_err(|error| RamAttachRefusal::Failed(error.to_string()))?;
        if !output.status.succeeded() {
            return Err(RamAttachRefusal::Failed(format!(
                "{command} ended {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        // `hdiutil attach -nomount ram://<n>` prints the whole disk it made, then spaces.
        let stdout = String::from_utf8_lossy(&output.stdout);
        RamDiskDevice::parse(stdout.split_whitespace().next().unwrap_or_default())
            .map(|device| RamDiskAttached { device })
            .map_err(|error| {
                RamAttachRefusal::Failed(format!(
                    "{command} printed no whole disk ({error}): {}",
                    stdout.trim()
                ))
            })
    }

    fn detach(
        &self,
        device: &RamDiskDevice,
        entry: RamDiskEntry,
        output_limit: usize,
    ) -> RamDetached {
        let request = CommandRequest::new(HDIUTIL, ["detach", device.as_str()]);
        let output =
            match self
                .runner
                .run_detach_of_entry(&request, entry.get(), Some(&self.lease_socket))
            {
                Ok(output) => output,
                Err(error) => {
                    let reason = error.to_string();
                    return match error.failure {
                        // Refused or failed before `hdiutil` executed: nothing was detached.
                        CommandRunFailure::Spawn(_)
                        | CommandRunFailure::VolumeRename
                        | CommandRunFailure::Release(_)
                        | CommandRunFailure::DeviceChanged(_)
                        | CommandRunFailure::Unspawned(_) => RamDetached::NotStarted { reason },
                        // `hdiutil` ran, and its end, or its output, was not observed.
                        CommandRunFailure::Wait(_)
                        | CommandRunFailure::Deadline { .. }
                        | CommandRunFailure::Kill { .. }
                        | CommandRunFailure::Reap { .. }
                        | CommandRunFailure::Input { .. } => RamDetached::Unknown { reason },
                    };
                }
            };
        let status = match output.status {
            ProcessStatus::Exit(code) => RamDetachStatus::Exit(code),
            ProcessStatus::Signal(signal) => RamDetachStatus::Signal(signal),
            ProcessStatus::Unknown => {
                return RamDetached::Unknown {
                    reason: format!(
                        "hdiutil detach {device} was reaped with neither an exit status nor a signal"
                    ),
                };
            }
        };
        RamDetached::terminal(status, &output.stdout, &output.stderr, output_limit)
    }
}

/// The host's physical memory in bytes (`hw.memsize`).
fn host_memory_bytes() -> std::io::Result<u64> {
    let mut bytes: u64 = 0;
    let mut size = std::mem::size_of::<u64>();
    // SAFETY: the name is NUL-terminated, and `bytes` and `size` are valid for the one u64
    // sysctl writes and the length it reads and writes back.
    let status = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            std::ptr::addr_of_mut!(bytes).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if status == 0 {
        Ok(bytes)
    } else {
        Err(std::io::Error::last_os_error())
    }
}
