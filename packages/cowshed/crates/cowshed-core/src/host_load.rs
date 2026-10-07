//! The host's run-queue load: checked job resource observations and the wider diagnostic
//! snapshot carried by a disk operation that host contention may have refused.
//!
//! The disk-images framework answers through helper daemons over XPC. Those daemons were seen to
//! drop whole bursts of concurrent `diskutil image attach` calls ("Couldn't communicate with a
//! helper application.") while the load average stood near 350; at load 125 the same attaches,
//! twelve at once, all succeeded. The command's own report says nothing about the host, so the
//! load at the failure travels with it.

use std::fmt;

use crate::api::resources::{HostLoadSample, ResourceUnitError};

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

#[derive(Debug, thiserror::Error)]
pub enum HostLoadError {
    #[error("getloadavg did not report the one-minute host load (returned {returned})")]
    LoadUnavailable { returned: libc::c_int },
    #[error("sysconf(_SC_NPROCESSORS_ONLN) did not report online host cores (returned {returned})")]
    CoresUnavailable { returned: libc::c_long },
    #[error("online host core count {value} exceeds the declared u16 range")]
    CoresOutOfRange { value: libc::c_long },
    #[error(transparent)]
    Unit(#[from] ResourceUnitError),
}

/// A current host snapshot for a job's spawn baseline or sample boundary.
pub fn read_host_load() -> Result<HostLoadSample, HostLoadError> {
    let mut load = [0.0; 1];
    // SAFETY: getloadavg writes at most one double into the one-element buffer.
    let written = unsafe { libc::getloadavg(load.as_mut_ptr(), 1) };
    // SAFETY: sysconf accepts this constant without any borrowed memory.
    let cores = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    sample_from_kernel(written, load[0], cores)
}

fn sample_from_kernel(
    written: libc::c_int,
    load: f64,
    cores: libc::c_long,
) -> Result<HostLoadSample, HostLoadError> {
    if written != 1 {
        return Err(HostLoadError::LoadUnavailable { returned: written });
    }
    if cores <= 0 {
        return Err(HostLoadError::CoresUnavailable { returned: cores });
    }
    let cores =
        u16::try_from(cores).map_err(|_| HostLoadError::CoresOutOfRange { value: cores })?;
    Ok(HostLoadSample::new(load, cores)?)
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

    #[test]
    fn resource_load_matches_independent_kernel_reads() {
        let before = HostLoad::read().expect("independent getloadavg before");
        let sample = read_host_load().expect("the host reports load and cores");
        let after = HostLoad::read().expect("independent getloadavg after");
        let load = sample.load1.get();
        let lower = before.one.min(after.one) - 0.01;
        let upper = before.one.max(after.one) + 0.01;
        assert!(
            (lower..=upper).contains(&load),
            "sample {load}, independent before {}, after {}",
            before.one,
            after.one
        );
        assert!(sample.cores.get() > 0);
    }

    #[test]
    fn resource_load_failure_is_not_a_zero_snapshot() {
        assert!(matches!(
            sample_from_kernel(-1, 0.0, 8),
            Err(HostLoadError::LoadUnavailable { returned: -1 })
        ));
        assert!(sample_from_kernel(0, 0.0, 8).is_err());
        assert!(sample_from_kernel(1, 0.0, -1).is_err());
        assert!(sample_from_kernel(1, 0.0, 0).is_err());
        assert!(sample_from_kernel(1, 0.0, 65_536).is_err());
        assert!(sample_from_kernel(1, f64::NAN, 8).is_err());
        assert!(sample_from_kernel(1, f64::INFINITY, 8).is_err());
        assert!(sample_from_kernel(1, -0.1, 8).is_err());
        let idle = sample_from_kernel(1, 0.0, 8).expect("real zero load is valid");
        assert_eq!(idle.load1.get(), 0.0);
        assert_eq!(idle.cores.get(), 8);
    }
}
