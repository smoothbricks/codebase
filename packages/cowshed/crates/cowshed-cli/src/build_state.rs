//! Read-only build-volume state for consuming-repository lint. No controller, store or discovery.

use std::fs;
use std::io::{self, Write};
use std::path::Path;

use cowshed_core::build_volume::{BUILD_LINK, BuildVolumeState};
use cowshed_core::capabilities::BuildStatePath;
use cowshed_core::{CowshedError, Result};
use serde::Serialize;

use crate::output::Output;

const MIGRATION_HINT: &str = "run `cowshed setup` (host) or any `cowshed exec` in it to migrate";

fn missing_volume() -> CowshedError {
    CowshedError::environment_missing(
        format!("this checkout has no build volume yet; {MIGRATION_HINT}"),
        MIGRATION_HINT,
    )
}

/// Read only the selected checkout's build link and volume-owned path record.
pub fn read(checkout: &Path) -> Result<BuildVolumeState> {
    let link = checkout.join(BUILD_LINK);
    let metadata = match fs::symlink_metadata(&link) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(missing_volume()),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "could not read build-volume link {}: {error}",
                    link.display()
                ),
                "make the checkout's build-volume link readable and retry",
            ));
        }
    };
    if !metadata.is_symlink() {
        return Err(CowshedError::integrity(
            format!("{} is not a build-volume link", link.display()),
            "restore the checkout's cowshed build-volume link before linting",
        ));
    }
    BuildVolumeState::read_optional(&link)?.ok_or_else(missing_volume)
}

#[derive(Serialize)]
struct Listing<'a> {
    paths: &'a [BuildStatePath],
}

pub fn dispatch<W: Write, E: Write>(
    checkout: &Path,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32> {
    let state = read(checkout)?;
    if json {
        output
            .bare_record(&Listing {
                paths: &state.paths,
            })
            .map_err(write_error)?;
    } else {
        for path in &state.paths {
            output
                .bare_line(
                    format!(
                        "{} -> {}",
                        path.checkout.as_path().display(),
                        path.volume.as_path().display()
                    )
                    .as_bytes(),
                )
                .map_err(write_error)?;
        }
    }
    Ok(0)
}

fn write_error(error: io::Error) -> CowshedError {
    CowshedError::internal(format!("write build-state paths: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_core::ErrorCode;
    use cowshed_core::build_volume::STATE_FILE;
    use std::os::unix::fs::symlink;

    struct Scratch(std::path::PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).expect("remove build-state test fixture");
        }
    }

    fn fixture() -> (Scratch, std::path::PathBuf, std::path::PathBuf) {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = Scratch(std::env::temp_dir().join(format!(
            "cowshed-build-state-{}-{nonce}",
            std::process::id()
        )));
        let checkout = root.0.join("checkout");
        let volume = root.0.join("volume");
        fs::create_dir_all(checkout.join(".cowshed")).unwrap();
        fs::create_dir(&volume).unwrap();
        (root, checkout, volume)
    }

    #[test]
    fn build_state_missing_volume_is_a_teaching_refusal_not_empty_paths() {
        let (_root, checkout, volume) = fixture();
        for linked in [false, true] {
            if linked {
                symlink(&volume, checkout.join(BUILD_LINK)).unwrap();
            }
            let error = read(&checkout).unwrap_err();
            assert_eq!(error.code, ErrorCode::EnvironmentMissing);
            assert_eq!(error.exit_code(), ErrorCode::EnvironmentMissing.exit_code());
            assert_eq!(
                error.message,
                format!("this checkout has no build volume yet; {MIGRATION_HINT}")
            );
            assert!(!volume.join(STATE_FILE).exists());
        }
    }

    #[test]
    fn build_state_json_uses_only_the_volume_record_and_does_not_rediscover() {
        let (_root, checkout, volume) = fixture();
        // This invalid manifest would make Cargo discovery fail if the read-only verb ran it.
        fs::write(checkout.join("Cargo.toml"), "not a cargo manifest").unwrap();
        symlink(&volume, checkout.join(BUILD_LINK)).unwrap();
        let state = BuildVolumeState {
            paths: vec![
                BuildStatePath::new("nested/out/cargo", "cargo/nested").unwrap(),
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
            ],
            fingerprint: Some("not part of the lint contract".to_owned()),
        };
        state.write(&volume).unwrap();
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        assert_eq!(dispatch(&checkout, true, &mut output).unwrap(), 0);
        let (stdout, stderr) = output.into_inner();
        assert!(stderr.is_empty());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&stdout).unwrap(),
            serde_json::json!({"paths": [
                {"checkout": "nested/out/cargo", "volume": "cargo/nested"},
                {"checkout": ".nx/cache", "volume": "nx/cache"}
            ]})
        );
        assert_eq!(read(&checkout).unwrap(), state);
    }

    #[test]
    fn build_state_refuses_foreign_real_directory_and_corrupt_state() {
        let (_root, checkout, volume) = fixture();
        fs::create_dir(checkout.join(BUILD_LINK)).unwrap();
        assert_eq!(read(&checkout).unwrap_err().code, ErrorCode::Integrity);
        fs::remove_dir(checkout.join(BUILD_LINK)).unwrap();
        symlink(&volume, checkout.join(BUILD_LINK)).unwrap();
        fs::write(volume.join(STATE_FILE), "corrupt").unwrap();
        assert_eq!(read(&checkout).unwrap_err().code, ErrorCode::Integrity);
        assert_eq!(
            fs::read_to_string(volume.join(STATE_FILE)).unwrap(),
            "corrupt"
        );
    }

    #[test]
    fn build_state_recorded_empty_set_is_not_a_missing_volume() {
        let (_root, checkout, volume) = fixture();
        symlink(&volume, checkout.join(BUILD_LINK)).unwrap();
        BuildVolumeState::default().write(&volume).unwrap();
        assert!(read(&checkout).unwrap().paths.is_empty());
    }
}
