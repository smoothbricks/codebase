//! Real APFS images for tests, cloned from one blank template per host user.
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
//! `#[path]`-included beside [`super::scratch_apfs`]. The template directory spells no pid, so no
//! sweep reclaims it.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use super::scratch_apfs::ROOT_PREFIX;
use super::{ApfsBackend, ImageCapacity, MacOsApfsBackend, SystemCommandRunner, blank_template};

/// The 1 GiB test cap (08_testing.md): the template's capacity, and so every clone's.
pub const CAPACITY: ImageCapacity = ImageCapacity::from_gibibytes(1);

/// The user's template, minted by whichever test asks first.
static TEMPLATE: LazyLock<PathBuf> = LazyLock::new(mint_once);

/// Put a formatted, detached, one-volume ASIF image of [`CAPACITY`] at `destination`, which must
/// not exist yet, creating its directory: an APFS clone of this run's template.
pub fn blank_image(destination: &Path) {
    let directory = destination.parent().expect("an image has a directory");
    fs::create_dir_all(directory)
        .unwrap_or_else(|error| panic!("image directory {}: {error}", directory.display()));
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
