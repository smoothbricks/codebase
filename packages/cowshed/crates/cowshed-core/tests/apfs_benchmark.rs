#![cfg(target_os = "macos")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use cowshed_core::apfs::{
    ApfsBackend, ApfsCaseSensitivity, AttachedImage, CreateImageRequest, ImageFormatSelection,
    MacOsApfsBackend, SystemCommandRunner,
};
use cowshed_core::metadata::ImageFormat;

#[test]
#[ignore = "explicit real APFS benchmark: nx benchmark-apfs cowshed"]
fn clonefile_and_attach_regression_budget() {
    assert_eq!(
        std::env::var("COWSHED_BENCH").as_deref(),
        Ok("1"),
        "invoke the explicit benchmark-apfs Nx target"
    );
    run_benchmark();
}

fn run_benchmark() {
    let required = std::env::var("COWSHED_APFS_REQUIRED").unwrap_or_else(|_| "auto".to_owned());
    assert!(
        matches!(required.as_str(), "auto" | "asif" | "sparse" | "both"),
        "COWSHED_APFS_REQUIRED must be auto|asif|sparse|both"
    );
    let formats: &[ImageFormat] = match required.as_str() {
        "asif" => &[ImageFormat::Asif],
        "sparse" => &[ImageFormat::Sparse],
        _ => &[ImageFormat::Sparse, ImageFormat::Asif],
    };
    let mut completed = 0;
    for &format in formats {
        match benchmark_format(format) {
            Ok(()) => completed += 1,
            Err(error) if required == "auto" => {
                eprintln!("APFS {format:?} benchmark unavailable in auto mode: {error}");
            }
            Err(error) => panic!("required APFS {format:?} benchmark failed: {error}"),
        }
    }
    assert!(completed > 0, "no APFS benchmark format completed");
    if required == "both" {
        assert_eq!(completed, 2, "both selected APFS formats must benchmark");
    }
}

struct BenchmarkRoot(PathBuf);

impl Drop for BenchmarkRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct AttachmentGuard<'a> {
    backend: &'a MacOsApfsBackend<SystemCommandRunner>,
    attachment: Option<AttachedImage>,
}

impl AttachmentGuard<'_> {
    fn detach(mut self) -> Result<(), cowshed_core::apfs::ApfsError> {
        let attachment = self.attachment.take().expect("attachment is present");
        self.backend.detach(&attachment, false)
    }
}

impl Drop for AttachmentGuard<'_> {
    fn drop(&mut self) {
        if let Some(attachment) = self.attachment.take() {
            let _ = self.backend.detach(&attachment, true);
        }
    }
}

fn benchmark_format(format: ImageFormat) -> Result<(), Box<dyn std::error::Error>> {
    let root = BenchmarkRoot(PathBuf::from(format!(
        "/private/tmp/cowshed-bench-{}-{}",
        std::process::id(),
        format.extension()
    )));
    if root.0.exists() {
        std::fs::remove_dir_all(&root.0)?;
    }
    std::fs::create_dir_all(&root.0)?;
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    let request = CreateImageRequest {
        staged_stem: root.0.join("source"),
        capacity: "1g".to_owned(),
        volume_name: format!("cowshed.bench.{}", format.extension()),
        case_sensitivity: ApfsCaseSensitivity::Insensitive,
        owner_uid: unsafe { libc::getuid() },
        owner_gid: unsafe { libc::getgid() },
        image_format: ImageFormatSelection::Exact(format),
    };
    let created = match backend.create_staged_image(&request) {
        Ok(created) => created,
        Err(error) => {
            return Err(Box::new(error));
        }
    };

    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut clone_samples = Vec::with_capacity(21);
        for index in 0..21 {
            let clone = root.0.join(format!("clone-{index}.{}", format.extension()));
            let started = Instant::now();
            backend.clone_image(&created.path, &clone, format)?;
            clone_samples.push(started.elapsed());
            backend.delete_image(&clone, format)?;
        }
        clone_samples.sort_unstable();
        let clone_median = clone_samples[clone_samples.len() / 2];
        let clone_max = *clone_samples.last().expect("clone samples");
        if clone_median >= Duration::from_millis(50) {
            return Err(format!("{format:?} clonefile median regressed: {clone_median:?}").into());
        }
        if clone_max >= Duration::from_millis(250) {
            return Err(format!("{format:?} clonefile max regressed: {clone_max:?}").into());
        }

        let mut attach_samples = Vec::with_capacity(10);
        for _ in 0..10 {
            let started = Instant::now();
            let attachment = backend.attach_verified(&created.path, format)?;
            let guard = AttachmentGuard {
                backend: &backend,
                attachment: Some(attachment),
            };
            attach_samples.push(started.elapsed());
            guard.detach()?;
        }
        attach_samples.sort_unstable();
        let attach_median = attach_samples[attach_samples.len() / 2];
        if attach_median >= Duration::from_secs(2) {
            return Err(
                format!("{format:?} attach+fsck median regressed: {attach_median:?}").into(),
            );
        }
        if clone_median >= attach_median {
            return Err("clonefile must remain cheaper than attach+fsck".into());
        }
        eprintln!(
            "APFS {format:?}: clone median={clone_median:?} max={clone_max:?}; attach+fsck median={attach_median:?}"
        );
        Ok(())
    })();

    let delete = backend.delete_image(&created.path, format);
    let remove_root = std::fs::remove_dir_all(&root.0);
    match (result, delete, remove_root) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(error), _, _) => Err(error),
        (_, Err(error), _) => Err(Box::new(error)),
        (_, _, Err(error)) => Err(Box::new(error)),
    }
}
