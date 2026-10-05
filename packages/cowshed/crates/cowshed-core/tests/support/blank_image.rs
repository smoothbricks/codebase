//! Real APFS images for tests that need one to attach, cloned from one template per test run.
//!
//! Minting an image costs a `diskutil image create` and a `diskutil image attach`, and both
//! queue on root `storagekitd`, which answers one caller host-wide at a time (01_storage.md, "How
//! the APFS host degrades"); a `newfs_apfs` and a detach follow. A test whose subject is what it
//! does with an image, not how the image came to be, gets an APFS clone of this run's template
//! instead: `clonefile` reaches no disk tool and no `newfs`. Tests that prove creation itself keep
//! creating through the production backend.
//!
//! `#[path]`-included beside [`super::scratch_apfs`], whose ownership protocol the template
//! directory follows.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use super::scratch_apfs::{ROOT_PREFIX, ScratchRoot, lock_exclusive};
use super::{
    ApfsBackend, CreateImageRequest, ImageCapacity, MacOsApfsBackend, SystemCommandRunner,
};

/// The template's volume name. A label is never an authority (01_storage.md, "Ownership,
/// identity, and the volume label"), so no test may depend on it.
const VOLUME_NAME: &str = "cowshed-itest";

/// The run's template, minted by whichever of its tests asks first.
static TEMPLATE: LazyLock<PathBuf> = LazyLock::new(mint_once);

/// Put a formatted, detached, one-volume ASIF image of the 1 GiB test cap (08_testing.md) at
/// `destination`, which must not exist yet: an APFS clone of this run's template.
pub fn blank_image(destination: &Path) {
    let template = TEMPLATE.as_path();
    MacOsApfsBackend::new(SystemCommandRunner)
        .clone_image(template, destination)
        .unwrap_or_else(|error| {
            panic!(
                "clone the run's blank image {} to {}: {error}",
                template.display(),
                destination.display()
            )
        });
}

/// Mint the run's template unless one of its tests already has.
///
/// The run is the process that spawned this one: nextest's runner, which starts every test in a
/// process of its own, or `cargo test`, whose one test process shares a single template among its
/// threads. The template directory spells that runner's pid, so it is the run's for as long as
/// the runner lives and the next run's sweep reclaims it afterwards. The image is minted in a
/// scratch root of the minting test's own and moved in only once it is complete and detached: a
/// minter killed halfway leaves its half-made image to the sweep, never in the template's place.
fn mint_once() -> PathBuf {
    // SAFETY: `getppid` has no preconditions and cannot fail.
    let run = unsafe { libc::getppid() };
    let directory = PathBuf::from(format!("{ROOT_PREFIX}{run}-templates"));
    fs::create_dir_all(&directory)
        .unwrap_or_else(|error| panic!("template directory {}: {error}", directory.display()));
    let template = directory.join("blank-1g.asif");
    let lock = directory.join("blank-1g.lock");
    let _minting = lock_exclusive(&lock)
        .unwrap_or_else(|error| panic!("template lock {}: {error}", lock.display()));
    if !template.exists() {
        let scratch = ScratchRoot::new("template").expect("template scratch root");
        let minted = MacOsApfsBackend::new(SystemCommandRunner)
            .create_staged_image(&CreateImageRequest {
                staged_stem: scratch.path().join("blank"),
                capacity: ImageCapacity::from_gibibytes(1),
                volume_name: VOLUME_NAME.to_owned(),
                // SAFETY: `getuid`/`getgid` read this process's credentials; they take no
                // pointers and cannot fail.
                owner_uid: unsafe { libc::getuid() },
                // SAFETY: as above.
                owner_gid: unsafe { libc::getgid() },
            })
            .expect("mint the run's blank image");
        fs::rename(&minted, &template).unwrap_or_else(|error| {
            panic!(
                "publish the run's blank image {} as {}: {error}",
                minted.display(),
                template.display()
            )
        });
    }
    template
}
