//! What a supervisor's socket takes from Linux: the build identity of the executed file.

use std::io;

use crate::api::dto::Sha256Digest;

/// The SHA-256 of the executable, read through `/proc/self/exe`, which names the executed file
/// even after its path is replaced. Read once per process.
pub(super) fn image_build_id() -> std::result::Result<String, String> {
    use sha2::Digest as _;
    let mut image = std::fs::File::open("/proc/self/exe")
        .map_err(|error| format!("cannot open /proc/self/exe: {error}"))?;
    let mut hasher = sha2::Sha256::new();
    io::copy(&mut image, &mut hasher)
        .map_err(|error| format!("cannot read /proc/self/exe: {error}"))?;
    Ok(Sha256Digest::from_bytes(hasher.finalize().into()).to_hex())
}
