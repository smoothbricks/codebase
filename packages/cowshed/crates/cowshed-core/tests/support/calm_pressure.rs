//! The memory-pressure feed a test supervisor runs on: one that only ever reports no pressure
//! (Normal on macOS, no stall on Linux), so the host's own pressure decides none of its admissions.
//! A test of admission under pressure drives its own feed (`Source::Driven`).

use cowshed_core::host_pressure::{Reading, Source};
use cowshed_gateway_types::host_status::MemoryPressure;

pub fn calm() -> Source {
    #[cfg(target_os = "macos")]
    let reading = Reading::Known(MemoryPressure::Darwin {
        level: cowshed_gateway_types::host_status::DarwinPressureLevel::Normal,
    });
    #[cfg(not(target_os = "macos"))]
    let reading = Reading::Known(MemoryPressure::Psi {
        some_avg10: cowshed_gateway_types::host_status::CentiPercent::new(0)
            .expect("no stall is a share"),
    });
    Source::Driven(tokio::sync::watch::channel(reading).1)
}
