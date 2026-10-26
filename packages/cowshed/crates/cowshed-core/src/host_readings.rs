//! The kernel readings `gateway status` reports (05_gateway.md "Host scheduling", "Status"):
//! load, memory pressure and swap. Each is read on its own, so one failed read never hides the
//! others, and a failed read is stated, never reported as zero.

use cowshed_gateway_types::host_status::{
    HostReadings, HostValueError, LoadAverage, LoadAverages, LoadReading, MemoryPressureReading,
    SwapReading,
};
use cowshed_os::host_load::HostLoad;

use crate::host_pressure::{read_memory_pressure, read_swap};

/// The host now, read by the process that reports it.
pub fn read() -> HostReadings {
    let load = HostLoad::read()
        .ok_or_else(|| "getloadavg did not report all three averages".to_owned())
        .and_then(|load| load_averages(load).map_err(|error| error.to_string()));
    HostReadings {
        load: match load {
            Ok(load) => LoadReading::Read { load },
            Err(error) => LoadReading::Unread { error },
        },
        memory_pressure: match read_memory_pressure() {
            Ok(pressure) => MemoryPressureReading::Read { pressure },
            Err(error) => MemoryPressureReading::Unread {
                error: error.to_string(),
            },
        },
        swap: match read_swap() {
            Ok(swap) => SwapReading::Read { swap },
            Err(error) => SwapReading::Unread {
                error: error.to_string(),
            },
        },
    }
}

fn load_averages(load: HostLoad) -> Result<LoadAverages, HostValueError> {
    Ok(LoadAverages {
        one: LoadAverage::new(load.one)?,
        five: LoadAverage::new(load.five)?,
        fifteen: LoadAverage::new(load.fifteen)?,
    })
}
