//! Real APFS images for tests, cloned from one blank template per host user, and the lane-wide
//! fixture pool that attaches each scenario image once per lane run.
//!
//! The template is a store's blank template (01_storage.md, "Images"), minted by the production
//! code into the user's template directory as if that directory were a store. A test that needs
//! an image gets an APFS clone of it: `clonefile` reaches no disk tool. A test whose store mints
//! through the production host seeds the store by putting a clone where the host looks for its
//! template (`blank_template_path`), so the store's mints clone the seed instead of each store
//! paying a `diskutil image create` and a formatting attach — both queued on root `storagekitd`
//! (01_storage.md, "How the APFS host degrades") — a `newfs_apfs` and a detach for a template of
//! its own. Tests that prove template minting itself leave the store unseeded.
//!
//! Seeding attaches nothing. The only attach in this support code is the fixture pool's: it
//! attaches each image in its registry once per lane run, under a lock shared by every test
//! process of the run, and hands tests handles to the already-attached images. A test therefore
//! cannot attach an image of its own through the fixture API — there is no per-test attach
//! helper in tests' reach — and the pool refuses to attach past its registry, so a new scenario
//! image is a deliberate registry entry, not an accident of a new test. The few tests that prove
//! attach or detach behaviour itself drive the production host directly and name that in their
//! docs. A later lane run detaches a previous run's pool images before attaching its own.
//!
//! `#[path]`-included beside [`super::scratch_apfs`]. The template directory spells no pid, so no
//! sweep reclaims it.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

use super::scratch_apfs::ROOT_PREFIX;
use super::{
    ApfsBackend, ApfsSubstrateConfig, CommandRunner, DetachIntent, DiskImageSource, ImageCapacity,
    ImageCapture, MacOsApfsBackend, MacOsApfsExecutionHost, SystemCommandRunner, blank_template,
};

/// The 1 GiB test cap (08_testing.md): the template's capacity, and so every clone's.
pub const CAPACITY: ImageCapacity = ImageCapacity::from_gibibytes(1);

/// The user's template, minted by whichever test asks first.
static TEMPLATE: LazyLock<PathBuf> = LazyLock::new(mint_once);

/// Refused once this boot's disk-image attach pool is spent (08_testing.md, "Host limit"):
/// after the first error code 150 of a run, every later fixture fails with
/// `AttachPoolExhausted` and its `next:` step instead of spending one more attach.
fn harness_gate() {
    if let Err(exhausted) = super::attach_budget::harness_gate() {
        panic!("{exhausted}");
    }
}

/// Put a formatted, detached, one-volume RAW image of [`CAPACITY`] at `destination`, which must
/// not exist yet, creating its directory: an APFS clone of this run's template. This is how a
/// store is seeded; it attaches nothing, so it is the only image a test may mint for itself.
pub fn blank_image(destination: &Path) {
    harness_gate();
    let directory = destination.parent().expect("an image has a directory");
    fs::create_dir_all(directory)
        .unwrap_or_else(|error| panic!("image directory {}: {error}", directory.display()));
    let template = TEMPLATE.as_path();
    MacOsApfsBackend::new(SystemCommandRunner)
        .clone_image(template, destination, ImageCapture::Detached)
        .unwrap_or_else(|error| {
            panic!(
                "clone the run's blank image {} to {}: {error}",
                template.display(),
                destination.display()
            )
        });
}

/// Mint the user's template unless an earlier test already has.
///
/// A template is a store's, and like a store's it outlives the process that minted it: its name
/// spells everything its bytes depend on (capacity, owner uid and gid), so every later run, and
/// every concurrent nextest lane, clones the same one. A run once minted its own, so each of a
/// gate's lanes paid a create, a formatting attach, a format and a detach before any of its tests
/// could start, all of it disk churn on the queue every other attach waits behind. Concurrent first
/// asks and a minter killed halfway are the production minter's to settle, under its lock.
fn mint_once() -> PathBuf {
    // SAFETY: `getuid` has no preconditions and cannot fail.
    let user = unsafe { libc::getuid() };
    let directory = PathBuf::from(format!("{ROOT_PREFIX}templates-uid{user}"));
    fs::create_dir_all(&directory)
        .unwrap_or_else(|error| panic!("template directory {}: {error}", directory.display()));
    blank_template(
        &MacOsApfsBackend::new(SystemCommandRunner),
        &directory,
        CAPACITY,
    )
    .unwrap_or_else(|error| panic!("mint the user's blank template: {error}"))
}

/// The fixture pool's scenario registry: every image the lane's shared fixtures attach, named
/// here and nowhere else. The lane's attach bound is this registry's size, computed from it —
/// never a measured constant.
const REGISTRY: &[&str] = &["scenario-main"];

/// How many images the lane's fixture pool attaches in one run: the registry's size.
pub fn fixture_registry_size() -> u64 {
    REGISTRY.len() as u64
}

/// This lane run's pool directory, shared by every test process of the run: nextest gives them
/// all one `NEXTEST_RUN_ID`; a `cargo test` run is one process, so its pid names it instead.
/// Its name spells no scratch-root pid, so no run's sweep reclaims another's pool.
fn pool_directory() -> PathBuf {
    let run =
        std::env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| format!("cargo-{}", std::process::id()));
    let run: String = run
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    PathBuf::from(format!("{ROOT_PREFIX}pool-{run}"))
}

/// A handle to one of the pool's scenario images, already attached by the pool. It hands out
/// the image's path and volume device and mounts that volume; it cannot attach or detach —
/// the pool owns the attachment for the whole lane run.
pub struct SharedImage {
    image: PathBuf,
    volume_device: String,
}

impl SharedImage {
    pub fn image(&self) -> &Path {
        &self.image
    }

    pub fn volume_device(&self) -> &str {
        &self.volume_device
    }

    /// Mount this image's volume at `mount_point` with `options` (as `mount_apfs -o` takes
    /// them): a mount of an already-attached volume, never an attach. The returned guard
    /// unmounts it; the image stays attached for the next test.
    pub fn mount_at(&self, mount_point: &Path, options: &str) -> SharedMount {
        fs::create_dir_all(mount_point)
            .unwrap_or_else(|error| panic!("mount point {}: {error}", mount_point.display()));
        let mounted = Command::new("/sbin/mount_apfs")
            .args(["-o", options, &self.volume_device])
            .arg(mount_point)
            .output()
            .expect("run mount_apfs");
        assert!(
            mounted.status.success(),
            "mount_apfs -o {options} {} {}: {}",
            self.volume_device,
            mount_point.display(),
            String::from_utf8_lossy(&mounted.stderr)
        );
        SharedMount {
            mount_point: mount_point.to_owned(),
        }
    }
}

/// One mount of a [`SharedImage`]'s volume; dropping it unmounts and nothing more.
pub struct SharedMount {
    mount_point: PathBuf,
}

impl SharedMount {
    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }
}

impl Drop for SharedMount {
    fn drop(&mut self) {
        let unmounted = Command::new("/sbin/umount")
            .arg(&self.mount_point)
            .output()
            .expect("run umount");
        if !unmounted.status.success() {
            eprintln!(
                "unmount shared fixture volume at {}: {}",
                self.mount_point.display(),
                String::from_utf8_lossy(&unmounted.stderr)
            );
        }
    }
}

/// This lane run's pool directory: the root under which shared scenario images live, laid out
/// by the tests that use them. Shared by every test process of the run.
pub fn pool_root() -> PathBuf {
    pool_directory()
}

/// The pool's scenario image named `scenario`, placed at `image` (a path under [`pool_root`]
/// the caller lays out), attached if this lane run has not attached it yet and shared with
/// every later test that asks. Panics if `scenario` is not in the registry, or if attaching it
/// would take the run past the registry's size: the lane assertion that a per-test attach
/// cannot sneak back in, enforced where the attach would be spent. A scenario whose image a
/// test detached while proving detach behaviour is re-attached by the next ask, replacing its
/// registry line; it is still the one scenario image.
pub fn shared_image_at(scenario: &str, image: &Path) -> SharedImage {
    assert!(
        REGISTRY.contains(&scenario),
        "no fixture-pool scenario named {scenario}; add it to the registry in blank_image.rs"
    );
    harness_gate();
    let pool = pool_directory();
    fs::create_dir_all(&pool)
        .unwrap_or_else(|error| panic!("pool directory {}: {error}", pool.display()));
    let _lock = super::scratch_apfs::lock_exclusive(&pool.join("pool.lock"))
        .unwrap_or_else(|error| panic!("pool lock: {error}"));
    detach_previous_runs(&pool);

    let registry_path = pool.join("registry.tsv");
    let recorded = fs::read_to_string(&registry_path).unwrap_or_default();
    let mut scenarios: Vec<&str> = recorded
        .lines()
        .map(|line| line.split('\t').next().expect("a scenario name"))
        .collect();
    for line in recorded.lines() {
        let mut fields = line.split('\t');
        if fields.next() == Some(scenario) {
            let recorded_image = PathBuf::from(fields.next().expect("a recorded image has a path"));
            let device = fields
                .next()
                .expect("a recorded image has a device")
                .to_owned();
            if recorded_image == image && kernel_holds(&recorded_image) {
                return SharedImage {
                    image: recorded_image,
                    volume_device: device,
                };
            }
        }
    }
    if !scenarios.contains(&scenario) {
        assert!(
            (scenarios.len() as u64) < fixture_registry_size(),
            "lane attach budget: the fixture pool already attached {} image(s) this run, \
             its whole registry of {}; a test must use a shared fixture, not attach another image",
            scenarios.len(),
            fixture_registry_size(),
        );
        scenarios.push(scenario);
    }

    if !image.exists() {
        blank_image(image);
    }
    let host = pool_host(&pool);
    let attachment = host
        .backend()
        .attach_verified(image)
        .unwrap_or_else(|error| panic!("attach the pool's {scenario} image: {error}"));
    let device = attachment.volume_device().to_owned();
    drop(attachment);
    let kept: Vec<&str> = recorded
        .lines()
        .filter(|line| line.split('\t').next() != Some(scenario))
        .collect();
    let mut rewritten = kept.join("\n");
    if !rewritten.is_empty() {
        rewritten.push('\n');
    }
    fs::write(
        &registry_path,
        format!("{rewritten}{scenario}\t{}\t{device}\n", image.display()),
    )
    .unwrap_or_else(|error| panic!("pool registry: {error}"));
    SharedImage {
        image: image.to_owned(),
        volume_device: device,
    }
}
/// Whether the kernel currently holds `image` attached.
fn kernel_holds(image: &Path) -> bool {
    SystemCommandRunner
        .attached_disk_images()
        .expect("kernel disk-image inventory")
        .iter()
        .any(|attached| matches!(&attached.source, DiskImageSource::File(path) if path == image))
}

/// A production host rooted at `pool`, for the pool's own attach and release work.
fn pool_host(pool: &Path) -> MacOsApfsExecutionHost<SystemCommandRunner> {
    MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(pool.to_path_buf(), pool.join("mount")),
    )
    .expect("fixture pool host")
}

/// Detach every image an earlier lane run's pool left attached and remove its directory: a
/// pool's images live exactly one run, and a later run is what ends an earlier one. Only a
/// pool untouched for an hour is ended, so a lane running concurrently is never touched.
fn detach_previous_runs(current: &Path) {
    let tmp = Path::new("/private/tmp");
    let Ok(entries) = fs::read_dir(tmp) else {
        return;
    };
    let host = pool_host(current);
    for entry in entries.flatten() {
        let path = entry.path();
        if path == *current
            || !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("cowshed-itest-pool-"))
        {
            continue;
        }
        let Ok(metadata) = fs::metadata(path.join("registry.tsv")) else {
            continue;
        };
        let stale = metadata
            .modified()
            .is_ok_and(|mtime| mtime.elapsed().is_ok_and(|age| age.as_secs() > 3600));
        if !stale {
            continue;
        }
        let Ok(recorded) = fs::read_to_string(path.join("registry.tsv")) else {
            continue;
        };
        for line in recorded.lines() {
            let image = PathBuf::from(line.split('\t').nth(1).unwrap_or_default());
            if kernel_holds(&image) {
                if let Err(error) = host.detach_existing_image(&image, DetachIntent::Release) {
                    eprintln!(
                        "release the previous run's pool image {}: {error}",
                        image.display()
                    );
                }
            }
        }
        let _ = fs::remove_dir_all(&path);
    }
}
