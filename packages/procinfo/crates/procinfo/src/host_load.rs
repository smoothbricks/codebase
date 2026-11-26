//! The host's run-queue load, carried by a disk operation that host contention may have refused.
//!
//! The disk-images framework answers through helper daemons over XPC. Those daemons were seen to
//! drop whole bursts of concurrent `diskutil image attach` calls ("Couldn't communicate with a
//! helper application.") while the load average stood near 350; at load 125 the same attaches,
//! twelve at once, all succeeded. The command's own report says nothing about the host, so the
//! load at the failure travels with it.

use std::fmt;

/// `getloadavg(3)`: the average run-queue length over the last 1, 5 and 15 minutes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HostLoad {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

impl HostLoad {
    /// The load now, or `None` when the kernel will not report all three averages.
    pub fn read() -> Option<Self> {
        let mut samples = [0f64; 3];
        // SAFETY: the buffer holds three doubles and getloadavg writes at most `nelem` of them.
        let written = unsafe { libc::getloadavg(samples.as_mut_ptr(), 3) };
        let [one, five, fifteen] = samples;
        (written == 3).then_some(Self { one, five, fifteen })
    }
}

impl fmt::Display for HostLoad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "loadavg {:.2} {:.2} {:.2}",
            self.one, self.five, self.fifteen
        )
    }
}

/// The host's cumulative CPU ticks across all online cores, as Mach `HOST_CPU_LOAD_INFO` counts
/// them (user, system, idle, nice).
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HostCpuTicks {
    pub user: u64,
    pub system: u64,
    pub idle: u64,
    pub nice: u64,
}

#[cfg(target_os = "macos")]
impl HostCpuTicks {
    /// Read the host's cumulative CPU ticks via `host_statistics(HOST_CPU_LOAD_INFO)`.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if the Mach host port cannot be obtained, `host_statistics`
    /// returns a non-success `kern_return_t`, or the kernel writes fewer than
    /// `HOST_CPU_LOAD_INFO_COUNT` fields.
    pub fn read() -> std::io::Result<Self> {
        unsafe extern "C" {
            static mach_task_self_: libc::mach_port_t;
            fn mach_host_self() -> libc::mach_port_t;
            fn mach_port_deallocate(
                task: libc::mach_port_t,
                name: libc::mach_port_t,
            ) -> libc::kern_return_t;
        }

        // SAFETY: `mach_host_self` takes no arguments and returns a send right to the host port.
        let host = unsafe { mach_host_self() };
        if host == 0 {
            return Err(std::io::Error::other(
                "mach_host_self returned MACH_PORT_NULL",
            ));
        }
        let mut info = std::mem::MaybeUninit::<libc::host_cpu_load_info>::zeroed();
        let mut count: libc::mach_msg_type_number_t = libc::HOST_CPU_LOAD_INFO_COUNT;
        // SAFETY: `info` is writable storage of `HOST_CPU_LOAD_INFO_COUNT` integers; `count` is
        // initialized to that capacity and updated in place by the kernel.
        let status = unsafe {
            libc::host_statistics(
                host,
                libc::HOST_CPU_LOAD_INFO,
                info.as_mut_ptr().cast(),
                &raw mut count,
            )
        };
        // SAFETY: `host` is the send right just returned by `mach_host_self`.
        unsafe {
            let _ = mach_port_deallocate(mach_task_self_, host);
        }
        if status != libc::KERN_SUCCESS {
            return Err(std::io::Error::other(format!(
                "host_statistics(HOST_CPU_LOAD_INFO) failed with kern_return_t {status}"
            )));
        }
        if count < libc::HOST_CPU_LOAD_INFO_COUNT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "host_statistics(HOST_CPU_LOAD_INFO) returned {count} fields, expected {}",
                    libc::HOST_CPU_LOAD_INFO_COUNT
                ),
            ));
        }
        // SAFETY: `host_statistics` returned `KERN_SUCCESS` and wrote all
        // `HOST_CPU_LOAD_INFO_COUNT` fields of `host_cpu_load_info`.
        let info = unsafe { info.assume_init() };
        let tick = |state: libc::c_int| -> std::io::Result<u64> {
            let index = usize::try_from(state).map_err(std::io::Error::other)?;
            info.cpu_ticks
                .get(index)
                .map(|&value| u64::from(value))
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("CPU state index {index} out of range"),
                    )
                })
        };
        Ok(Self {
            user: tick(libc::CPU_STATE_USER)?,
            system: tick(libc::CPU_STATE_SYSTEM)?,
            idle: tick(libc::CPU_STATE_IDLE)?,
            nice: tick(libc::CPU_STATE_NICE)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_kernel_reports_all_three_averages() {
        let load = HostLoad::read().expect("getloadavg answers on every supported host");
        for average in [load.one, load.five, load.fifteen] {
            assert!(average.is_finite() && average >= 0.0, "{load}");
        }
    }

    #[test]
    fn the_report_names_each_window() {
        let load = HostLoad {
            one: 351.43,
            five: 280.876,
            fifteen: 7.0,
        };
        assert_eq!(load.to_string(), "loadavg 351.43 280.88 7.00");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn host_cpu_ticks_read_as_cumulative_counters() {
        let total = |ticks: HostCpuTicks| {
            ticks
                .user
                .saturating_add(ticks.system)
                .saturating_add(ticks.idle)
                .saturating_add(ticks.nice)
        };
        let before = HostCpuTicks::read().expect("HOST_CPU_LOAD_INFO answers on macOS");
        assert!(
            total(before) > 0,
            "expected non-zero cumulative CPU ticks: {before:?}"
        );
        let after = HostCpuTicks::read().expect("HOST_CPU_LOAD_INFO answers on macOS");
        assert!(
            after.user >= before.user
                && after.system >= before.system
                && after.idle >= before.idle
                && after.nice >= before.nice,
            "cumulative CPU ticks never fall: before={before:?}, after={after:?}"
        );
    }
}
