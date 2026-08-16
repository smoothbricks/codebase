#[cfg(target_os = "macos")]
mod macos {
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use cowshed_core::apfs::{
        ApfsBackend, ApfsCaseSensitivity, AttachedImage, CreateImageRequest, ImageFormatSelection,
        MacOsApfsBackend, SystemCommandRunner,
    };
    use cowshed_core::metadata::{ImageCapacity, ImageFormat};

    pub fn run() {
        let formats = [ImageFormat::Sparse, ImageFormat::Asif];
        let mut completed = 0;
        for format in formats {
            benchmark_format(format)
                .unwrap_or_else(|error| panic!("APFS {format:?} benchmark failed: {error}"));
            completed += 1;
        }
        assert_eq!(
            completed,
            formats.len(),
            "both APFS formats must complete the benchmark"
        );
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
            let result = self.backend.detach(
                self.attachment.as_ref().expect("attachment is present"),
                false,
            );
            if result.is_ok() {
                self.attachment = None;
            }
            result
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
            capacity: ImageCapacity::from_gibibytes(1),
            volume_name: format!("cowshed.bench.{}", format.extension()),
            case_sensitivity: ApfsCaseSensitivity::Insensitive,
            owner_uid: unsafe { libc::getuid() },
            owner_gid: unsafe { libc::getgid() },
            image_format: ImageFormatSelection::Exact(format),
        };
        let created = backend.create_staged_image(&request)?;

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
                return Err(
                    format!("{format:?} clonefile median regressed: {clone_median:?}").into(),
                );
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
        let mut failures = Vec::new();
        if let Err(error) = result {
            failures.push(format!("benchmark failed: {error}"));
        }
        if let Err(error) = delete {
            failures.push(format!("source image cleanup failed: {error}"));
        }
        if let Err(error) = remove_root {
            failures.push(format!("temporary directory cleanup failed: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; ").into())
        }
    }
}

#[cfg(target_os = "macos")]
fn main() {
    macos::run();
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("the APFS benchmark requires macOS");
    std::process::exit(1);
}
