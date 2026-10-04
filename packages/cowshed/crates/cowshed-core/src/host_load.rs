//! The host's run-queue load, read where a failed disk operation may have been refused by host
//! contention rather than by anything in its request.
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
}
