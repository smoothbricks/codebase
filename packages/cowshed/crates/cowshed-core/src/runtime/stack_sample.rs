//! The unprivileged macOS stack sampler a wait recording takes a silent workspace supervisor's or
//! cowshed controller's stacks with ([`super::wait_spans::WaitRecording`]).

#[cfg(target_os = "macos")]
pub fn capture(pid: i32) -> Result<Vec<u8>, String> {
    use cowshed_os::deadline::{self, Bound, LatencyClass};
    // Two seconds of stacks, with the existing 10-second ffi class for the helper's whole run.
    let bound = Bound::new(LatencyClass::Ffi, deadline::LAUNCHCTL.within);
    let output = deadline::output_within(
        "silent peer stack sampler",
        std::process::Command::new("/usr/bin/sample")
            .arg(pid.to_string())
            .args(["2", "-mayDie", "-file", "/dev/stdout"]),
        &deadline::start("sample silent peer", bound),
    )
    .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "sample pid {pid} ended {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    if output.stdout.is_empty() {
        return Err(format!(
            "sample pid {pid} returned no stacks: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output.stdout)
}

#[cfg(not(target_os = "macos"))]
pub fn capture(pid: i32) -> Result<Vec<u8>, String> {
    Err(format!(
        "stack sampling pid {pid} is unsupported on this platform"
    ))
}
