//! The host's run-queue load as a checked job resource observation.

use crate::api::resources::{HostLoadSample, ResourceUnitError};

#[derive(Clone, Debug, thiserror::Error)]
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
    fn resource_load_matches_independent_kernel_reads() {
        let before =
            cowshed_os::host_load::HostLoad::read().expect("independent getloadavg before");
        let sample = read_host_load().expect("the host reports load and cores");
        let after = cowshed_os::host_load::HostLoad::read().expect("independent getloadavg after");
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
