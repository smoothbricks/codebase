//! Linux's memory pressure and swap, as procfs publishes them (05_gateway.md "Host scheduling",
//! "Memory pressure", "Status").
//!
//! - Memory pressure is `/proc/pressure/memory`'s `some avg10`: the share of the last ten seconds in
//!   which at least one task stalled on memory (psi.rst). A kernel without PSI (`CONFIG_PSI` off,
//!   or booted `psi=0`) has no such file: that is an error, never an unpressured host.
//! - Swap is `/proc/meminfo`'s `SwapTotal` and `SwapTotal - SwapFree`.
//!
//! Each file is read whole into a stack buffer, so a reading allocates only the text of a
//! malformed one.

use std::fs::File;
use std::io::{ErrorKind, Read as _};

use cowshed_gateway_types::host_status::{CentiPercent, MemoryPressure, SwapUsage};

use super::HostPressureError;

const MEMORY_PRESSURE: &str = "/proc/pressure/memory";
const MEMINFO: &str = "/proc/meminfo";

/// Room for either file whole: `/proc/meminfo` is about 1.5 KiB on Linux 6.x, the PSI file two lines.
const BUFFER: usize = 8192;

/// The host's memory pressure: `some avg10` of `/proc/pressure/memory`.
pub fn read_memory_pressure() -> Result<MemoryPressure, HostPressureError> {
    let mut buffer = [0; BUFFER];
    let text = read(MEMORY_PRESSURE, &mut buffer)?;
    some_avg10(text)
        .and_then(|value| CentiPercent::new(value).ok())
        .map(|some_avg10| MemoryPressure::Psi { some_avg10 })
        .ok_or_else(|| malformed(MEMORY_PRESSURE, text))
}

/// The host's swap: `SwapTotal` and what of it is not `SwapFree`.
pub fn read_swap() -> Result<SwapUsage, HostPressureError> {
    let mut buffer = [0; BUFFER];
    let text = read(MEMINFO, &mut buffer)?;
    let total = meminfo_bytes(text, "SwapTotal");
    let free = meminfo_bytes(text, "SwapFree");
    total
        .zip(free)
        .and_then(|(total, free)| Some((total.checked_sub(free)?, total)))
        .and_then(|(used, total)| SwapUsage::new(used, total).ok())
        .ok_or_else(|| malformed(MEMINFO, text))
}

fn malformed(file: &'static str, text: &str) -> HostPressureError {
    HostPressureError::Malformed {
        file,
        text: text.to_owned(),
    }
}

/// `file` whole, as text, in `buffer`. A file that fills the buffer is malformed rather
/// than read in part: a cut `/proc/meminfo` could lose the line asked for.
fn read<'b>(
    file: &'static str,
    buffer: &'b mut [u8; BUFFER],
) -> Result<&'b str, HostPressureError> {
    let failed = |cause| HostPressureError::Read { file, cause };
    let mut opened = File::open(file).map_err(failed)?;
    let mut filled = 0;
    loop {
        match opened.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(cause) => return Err(failed(cause)),
        }
        if filled == BUFFER {
            return Err(HostPressureError::Malformed {
                file,
                text: format!("longer than {BUFFER} bytes"),
            });
        }
    }
    let read = &buffer[..filled];
    std::str::from_utf8(read).map_err(|_| malformed(file, &String::from_utf8_lossy(read)))
}

/// A run of ASCII digits as a number; anything else, a sign included, is none.
fn digits<T: std::str::FromStr>(text: &str) -> Option<T> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// `some avg10` in hundredths of a percent. The kernel prints every average as `%lu.%02lu`
/// (kernel/sched/psi.c, `psi_show`), so `avg10=12.34` is 1234.
fn some_avg10(text: &str) -> Option<u16> {
    let fields = text.lines().find_map(|line| line.strip_prefix("some "))?;
    let value = fields
        .split_whitespace()
        .find_map(|field| field.strip_prefix("avg10="))?;
    let (whole, hundredths) = value.split_once('.')?;
    if hundredths.len() != 2 {
        return None;
    }
    digits::<u16>(whole)?
        .checked_mul(100)?
        .checked_add(digits::<u16>(hundredths)?)
}

/// The `key` line of `/proc/meminfo` in bytes: the kernel prints `Key:  <n> kB` in KiB
/// (fs/proc/meminfo.c).
fn meminfo_bytes(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let kib = line
            .strip_prefix(key)?
            .strip_prefix(':')?
            .trim_start()
            .strip_suffix(" kB")?;
        digits::<u64>(kib)?.checked_mul(1024)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn some_avg10_is_read_in_hundredths_of_a_percent() {
        let text = "some avg10=12.34 avg60=1.00 avg300=0.50 total=987\n\
                    full avg10=99.99 avg60=0.00 avg300=0.00 total=1\n";
        assert_eq!(some_avg10(text), Some(1234));
        assert_eq!(some_avg10("some avg10=100.00 avg60=0.00\n"), Some(10_000));
        assert_eq!(
            some_avg10("full avg10=12.34\n"),
            None,
            "only the some line counts"
        );
        assert_eq!(some_avg10("some avg10=1.5 avg60=0.00\n"), None);
        assert_eq!(some_avg10("some avg10=+1.50 avg60=0.00\n"), None);
    }

    #[test]
    fn a_meminfo_line_is_read_in_bytes_by_its_exact_key() {
        let text =
            "SwapCached:        1024 kB\nSwapTotal:      8388604 kB\nSwapFree:       8388000 kB\n";
        assert_eq!(meminfo_bytes(text, "SwapTotal"), Some(8_388_604 * 1024));
        assert_eq!(meminfo_bytes(text, "SwapFree"), Some(8_388_000 * 1024));
        assert_eq!(
            meminfo_bytes(text, "Swap"),
            None,
            "a key is matched whole, never by prefix"
        );
    }
}
