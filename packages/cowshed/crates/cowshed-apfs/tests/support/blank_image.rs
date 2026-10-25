//! Real APFS images for tests, cloned from one blank template per host user, and the lane-wide
//! fixture pool that attaches each scenario image once per lane run.
//!
//! The template is a store's blank template (01a_apfs.md, "Images"), minted by the production
//! code into the user's template directory as if that directory were a store. A test that needs
//! an image gets an APFS clone of it: `clonefile` reaches no disk tool. A test whose store mints
//! through the production host seeds the store by putting a clone where the host looks for its
//! template (`blank_template_path`), so the store's mints clone the seed instead of each store
//! paying a formatting attach, a `newfs_apfs` and a detach for a template of its own. Tests that prove template minting itself leave the store unseeded.
//!
//! Seeding attaches nothing. The only attach in this support code is the fixture pool's: it
//! attaches each image in its registry once per lane run, under a lock shared by every test
//! process of the run, and hands tests handles to the already-attached images. A test therefore
//! cannot attach an image of its own through the fixture API — there is no per-test attach
//! helper in tests' reach — and the pool refuses to attach past its registry, so a new scenario
//! image is a deliberate registry entry, not an accident of a new test. The few tests that prove
//! attach or detach behaviour itself drive the production host directly and name that in their
//! docs. A later lane run detaches a previous run's pool images before attaching its own.
//! Mount and unmount output capture uses the fork-safe `Run::output_locked` path, so fixture
//! commands obey the same process-launch lock as production commands.
//!
//! `#[path]`-included beside [`super::scratch_apfs`]. The template directory lies in `t`, beside no
//! test namespace, and its name spells the template's label, so no sweep reclaims it and a template
//! of another label is never found in it.

// The pool's mount/umount run in the test process, not under a supervisor: no fork is in flight.
#![allow(dead_code, clippy::disallowed_methods)]

use std::fs::{self, File};
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use cowshed_os::fork_lock::Run as _;
use cowshed_os::namespace::{HostNamespace, TestNamespace};

use super::scratch_apfs::{SCRATCH_PARENT, release_image};
use super::{
    ApfsBackend, CommandRunner, DiskImageSource, ImageCapacity, ImageCapture, MacOsApfsBackend,
    SystemCommandRunner, blank_template,
};

/// The 1 GiB test cap (08_testing.md): the template's capacity, and so every clone's.
pub const CAPACITY: ImageCapacity = ImageCapacity::from_gibibytes(1);

/// The label of the user's blank template, and so of every image cloned from it until its
/// workspace's supervisor names it. The template is every test run's, not one test namespace's, so
/// no namespace's `[cowshed-test-<ns>]` is its label; it is never production's `[cowshed]`
/// either. A test that asserts the label of a volume it seeded from the template asserts this one.
/// The template's directory spells it, so a template of any other label is not found there.
pub const SHARED_TEMPLATE_LABEL: &str = "[cowshed-test]";

/// The runner of the disk tools this support code runs for the fixture of `namespace`.
fn runner(namespace: TestNamespace) -> SystemCommandRunner {
    SystemCommandRunner::new(HostNamespace::Test(namespace))
}

/// The fixture's taker of the template's lock. It records its pid and its one request, minting
/// the template, on the lock, and names a holder it waits for only as the one kind of process that
/// takes that lock: this crate has no kernel census of a lock's holders, which the production
/// taker (cowshed-core's `held_lock::ImageLockTaker`) reads.
struct TemplateTaker;

impl cowshed_apfs::store_lock::Taker for TemplateTaker {
    fn record(&self, file: &File) -> std::io::Result<()> {
        file.set_len(0)?;
        let record = format!(
            r#"{{"pid":{},"operation":"mint-template","workspace":null}}"#,
            std::process::id()
        );
        file.write_all_at(record.as_bytes(), 0)
    }

    fn holders(&self, _: &Path) -> String {
        "another test process minting the template".to_owned()
    }
}

/// The user's template, minted by whichever test asks first.
static TEMPLATE: OnceLock<PathBuf> = OnceLock::new();

/// Put a formatted, detached, one-volume RAW image of [`CAPACITY`] at `destination`, which must
/// not exist yet, creating its directory: an APFS clone of this run's template, which the fixture
/// of `namespace` mints if it asks first. This is how a store is seeded; it attaches nothing, so
/// it is the only image a test may mint for itself.
pub fn blank_image(namespace: TestNamespace, destination: &Path) {
    let directory = destination.parent().expect("an image has a directory");
    fs::create_dir_all(directory)
        .unwrap_or_else(|error| panic!("image directory {}: {error}", directory.display()));
    let template = TEMPLATE.get_or_init(|| mint_once(namespace)).as_path();
    MacOsApfsBackend::new(runner(namespace))
        .clone_image(template, destination, ImageCapture::Detached)
        .unwrap_or_else(|error| {
            panic!(
                "clone the run's blank image {} to {}: {error}",
                template.display(),
                destination.display()
            )
        });
}

/// Mint the user's template, as the fixture of `namespace`, unless an earlier test already has.
///
/// A template is a store's, and like a store's it outlives the process that minted it: its name
/// spells everything its bytes depend on (capacity, owner uid and gid, and, in its directory's
/// name, its label), so every later run, and every concurrent nextest lane, clones the same one. A
/// run once minted its own, so each of a gate's lanes paid a create, a formatting attach, a format
/// and a detach before any of its tests could start, all of it disk churn on the queue every other
/// attach waits behind. Concurrent first asks and a minter killed halfway are the production
/// minter's to settle, under its lock.
fn mint_once(namespace: TestNamespace) -> PathBuf {
    // SAFETY: `getuid` has no preconditions and cannot fail.
    let user = unsafe { libc::getuid() };
    let directory = SCRATCH_PARENT.join(format!(
        "templates-uid{user}-{}",
        SHARED_TEMPLATE_LABEL
            .trim_start_matches('[')
            .trim_end_matches(']')
    ));
    fs::create_dir_all(&directory)
        .unwrap_or_else(|error| panic!("template directory {}: {error}", directory.display()));
    blank_template(
        &MacOsApfsBackend::new(runner(namespace)),
        &directory,
        SHARED_TEMPLATE_LABEL,
        CAPACITY,
        &TemplateTaker,
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
/// It lies in `t`, beside no test namespace, so no run's sweep reclaims another's pool.
fn pool_directory() -> PathBuf {
    let run =
        std::env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| format!("cargo-{}", std::process::id()));
    let run: String = run
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    SCRATCH_PARENT.join(format!("pool-{run}"))
}

/// A handle to one of the pool's scenario images, already attached by the pool. It hands out
/// the image's path and volume device and mounts that volume; it cannot attach or detach —
/// the pool owns the attachment for the whole lane run. Its mounts run their disk tools as the
/// namespace of the fixture that asked for it.
pub struct SharedImage {
    image: PathBuf,
    volume_device: String,
    namespace: TestNamespace,
}

impl SharedImage {
    pub fn image(&self) -> &Path {
        &self.image
    }

    pub fn volume_device(&self) -> &str {
        &self.volume_device
    }

    /// Mount this image's exact pool-owned volume at `mount_point` with mount_apfs, as production
    /// mounts (01a_apfs.md, "Mounts"), with `options` handed verbatim to the filesystem helper:
    /// a mount of an already-attached volume, never an attach or cached-family selection.
    /// The options are the caller's whole flag set, so a test can mount the one shared volume
    /// under any combination. The returned guard unmounts it; the image stays attached for the
    /// next test.
    pub fn mount_at(&self, mount_point: &Path, options: &str) -> SharedMount {
        fs::create_dir_all(mount_point)
            .unwrap_or_else(|error| panic!("mount point {}: {error}", mount_point.display()));
        let mounted = disk_tool(
            self.namespace,
            "/sbin/mount_apfs",
            &[
                "-o".as_ref(),
                options.as_ref(),
                self.volume_device.as_ref(),
                mount_point.as_os_str(),
            ],
        );
        assert!(
            mounted.status.success(),
            "mount_apfs -o {options} {} {}: {}",
            self.volume_device,
            mount_point.display(),
            String::from_utf8_lossy(&mounted.stderr)
        );
        SharedMount {
            image: self.image.clone(),
            mount_point: mount_point.to_owned(),
            namespace: self.namespace,
        }
    }
}

/// One mount of a [`SharedImage`]'s volume; dropping it unmounts and nothing more.
pub struct SharedMount {
    image: PathBuf,
    mount_point: PathBuf,
    namespace: TestNamespace,
}

impl SharedMount {
    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }
}

impl Drop for SharedMount {
    fn drop(&mut self) {
        let backend = MacOsApfsBackend::new(runner(self.namespace));
        let unmounted = backend
            .existing_attachment(&self.image)
            .and_then(|attachment| match attachment {
                Some(attachment) => {
                    backend.unmount_verified(&attachment, cowshed_apfs::DetachIntent::WhenIdle)
                }
                None => Ok(()),
            });
        if let Err(error) = unmounted {
            cowshed_os::eprintln!(
                "unmount shared fixture volume at {}: {error}",
                self.mount_point.display()
            );
        }
    }
}

/// Run the disk tool `program args` as production runs one, for the fixture of `namespace`: under
/// that namespace's disk-lifecycle lease, which its fixture gateway serves at
/// `<namespace root>/gateway.sock` and nothing serves otherwise, so the tool then runs unleased;
/// never under production's. It is also appended to the run's disk command record, through the
/// fork-safe launch.
fn disk_tool(
    namespace: TestNamespace,
    program: &str,
    args: &[&std::ffi::OsStr],
) -> std::process::Output {
    let program = Path::new(program);
    cowshed_apfs::disk_lease::leased(HostNamespace::Test(namespace), program, args, None, || {
        Command::new(program).args(args).output_locked()
    })
    .unwrap_or_else(|error| panic!("run {}: {error}", program.display()))
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
/// registry line; it is still the one scenario image. Its attach, the clone of a missing image and
/// the release of an earlier run's pool run as `namespace`, the asking fixture's, and so do the
/// handle's mounts.
pub fn shared_image_at(namespace: TestNamespace, scenario: &str, image: &Path) -> SharedImage {
    assert!(
        REGISTRY.contains(&scenario),
        "no fixture-pool scenario named {scenario}; add it to the registry in blank_image.rs"
    );
    let pool = pool_directory();
    fs::create_dir_all(&pool)
        .unwrap_or_else(|error| panic!("pool directory {}: {error}", pool.display()));
    let _lock = super::scratch_apfs::lock_exclusive(&pool.join("pool.lock"))
        .unwrap_or_else(|error| panic!("pool lock: {error}"));
    detach_previous_runs(namespace, &pool);

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
            if recorded_image == image && kernel_holds(namespace, &recorded_image) {
                return SharedImage {
                    image: recorded_image,
                    volume_device: device,
                    namespace,
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
        blank_image(namespace, image);
    }
    // Attached without a mount on purpose: every test of the run shares this one volume and
    // mounts it at its own path with its own flags through `SharedImage::mount_at`, so no single
    // mount point belongs to the attach.
    let attachment = MacOsApfsBackend::new(runner(namespace))
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
        namespace,
    }
}
/// Whether the kernel currently holds `image` attached.
fn kernel_holds(namespace: TestNamespace, image: &Path) -> bool {
    runner(namespace)
        .attached_disk_images()
        .expect("kernel disk-image inventory")
        .iter()
        .any(|attached| matches!(&attached.source, DiskImageSource::File(path) if path == image))
}

/// Detach every image an earlier lane run's pool left attached and remove its directory: a
/// pool's images live exactly one run, and a later run is what ends an earlier one. Only a
/// pool untouched for an hour is ended, so a lane running concurrently is never touched. A pool
/// belongs to a run, not to a namespace, so its images are released as `namespace`, the asking
/// fixture's.
fn detach_previous_runs(namespace: TestNamespace, current: &Path) {
    let Ok(entries) = fs::read_dir(SCRATCH_PARENT.as_path()) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == *current
            || !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("pool-"))
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
            if kernel_holds(namespace, &image)
                && let Err(error) = release_image(namespace, &image)
            {
                cowshed_os::eprintln!(
                    "release the previous run's pool image {}: {error}",
                    image.display()
                );
            }
        }
        let _ = fs::remove_dir_all(&path);
    }
}
