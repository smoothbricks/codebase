//! ZFS datasets as the payloads of a directory store (specs/cowshed/01b_zfs.md).
//!
//! A payload is one dataset, mounted wherever the store has the payload's directory: at its image
//! path while it is home, at its mountpoint while it is mounted. The dataset's `mountpoint`
//! property is therefore the payload's place, and "moving" a payload is setting it. Everything
//! else the store keeps (sidecars, locks, mount records, the trash grammar) is unchanged, so the
//! directory lifecycle runs over datasets as it runs over directories; only making, cloning,
//! moving and destroying a payload differ.
//!
//! Which dataset a payload is comes from the kernel: the source of the mount at its directory
//! ([`cowshed_helper::host::mounted_dataset`]). Nothing records it. Reads (`zfs get`, `zfs list`)
//! run unprivileged; every mutation goes through `cowshed-helper` (09_substrates.md, "Privileged
//! helper").

use std::fs::{self, File};
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use cowshed_helper::name::{Dataset, Root};
use cowshed_os::deadline::{self, Bound, LatencyClass};

use super::{MountPointState, Payload, clear_move_sibling, mount_point_state, move_sibling};
use crate::repository::SESSIONS_DIRECTORY;
use crate::storage::apfs::native::sync_parent_path;
use crate::storage::apfs::{ApfsStorageError, combine_cleanup};
use cowshed_apfs::image::IMAGE_EXTENSION;

/// One `zfs` command, or one helper invocation: a metadata transaction on the pool. A destroy
/// frees its blocks in the background (01b_zfs.md, "Destroy does not stall the pool"), so none of
/// them walks data.
const ZFS: Bound = Bound::new(LatencyClass::Ffi, Duration::from_secs(60));

const HELPER: &str = "cowshed-helper";

/// How the helper gets its privilege.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Elevation {
    /// The helper is installed setuid root.
    Setuid,
    /// Through `sudo -n`, by the sudoers entry that names the helper.
    Sudo,
}

/// A root-owned file with the setuid bit elevates itself; anything else is run through `sudo`.
fn elevation(owner: u32, mode: u32) -> Elevation {
    if owner == 0 && mode & 0o4000 != 0 {
        Elevation::Setuid
    } else {
        Elevation::Sudo
    }
}

/// The helper that serves, of the helper files there are in search order (beside cowshed, then
/// each `PATH` directory) with how each gets its privilege: the first that elevates itself, an
/// administrator's install that needs nothing more, else the first there is, which `sudo` must
/// admit (09_substrates.md, "Privileged helper").
fn choose_helper<P>(candidates: impl IntoIterator<Item = (P, Elevation)>) -> Option<P> {
    let mut first = None;
    for (candidate, elevation) in candidates {
        if elevation == Elevation::Setuid {
            return Some(candidate);
        }
        first.get_or_insert(candidate);
    }
    first
}

/// The helper files in `directories`, in order, each with how it gets its privilege. A relative
/// directory is passed over: neither `sudo` nor a daemon's working directory resolves it.
fn helpers_in(
    directories: impl IntoIterator<Item = PathBuf>,
) -> impl Iterator<Item = (PathBuf, Elevation)> {
    directories
        .into_iter()
        .filter(|directory| directory.is_absolute())
        .filter_map(|directory| {
            let candidate = directory.join(HELPER);
            let metadata = fs::metadata(&candidate)
                .ok()
                .filter(fs::Metadata::is_file)?;
            let elevation = elevation(metadata.uid(), metadata.permissions().mode());
            Some((candidate, elevation))
        })
}

/// The dataset name of the payload at `project_relative`, below its project's container: the
/// payload's place in the store, with the store's `sessions` directory named `ws` and no image
/// extension (01b_zfs.md, "One pool, one root"). `main.dmg` is `main`, `sessions/raven.dmg` is
/// `ws/raven`, `sessions/.trash/raven-<incarnation>.dmg` is `ws/.trash/raven-<incarnation>` and
/// `build/<id>.dmg` is `build/<id>`. A dataset is therefore found by name from the path that
/// names its payload, and `zfs list` reads as the store does.
fn place_name(project_relative: &Path) -> Option<Vec<String>> {
    let mut parts: Vec<String> = project_relative
        .components()
        .map(|part| part.as_os_str().to_str().map(dataset_component))
        .collect::<Option<_>>()?;
    let last = parts.pop()?;
    let stem = last.strip_suffix(&format!(".{IMAGE_EXTENSION}"))?;
    if stem.is_empty() {
        return None;
    }
    if let Some(first) = parts.first_mut()
        && first == SESSIONS_DIRECTORY
    {
        *first = "ws".to_owned();
    }
    parts.push(stem.to_owned());
    Some(parts)
}

/// A store path component as a dataset name component. The store's encoding escapes with `%`,
/// which a dataset name cannot hold and `:` can; the encoding never emits `:` itself.
fn dataset_component(encoded: &str) -> String {
    encoded.replace('%', ":")
}

fn host(context: impl std::fmt::Display, cause: impl std::fmt::Display) -> ApfsStorageError {
    ApfsStorageError::Host(format!("{context}: {cause}"))
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> ApfsStorageError {
    ApfsStorageError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn path_text(path: &Path) -> Result<&str, ApfsStorageError> {
    path.to_str()
        .ok_or_else(|| host("a ZFS mountpoint must be UTF-8", path.display()))
}

/// The datasets of one cowshed root, as payloads of the store at `store_root`.
#[derive(Clone, Debug)]
pub struct ZfsPayloads {
    root: Root,
    store_root: PathBuf,
    helper: PathBuf,
}

/// The directory of this executable, through the links that name it: the helper is installed
/// beside the executable itself.
fn beside_this_executable() -> Option<PathBuf> {
    std::env::current_exe()
        .and_then(fs::canonicalize)
        .ok()
        .and_then(|executable| Some(executable.parent()?.to_owned()))
}

fn run(role: &'static str, mut command: Command) -> Result<String, ApfsStorageError> {
    let described = format!("{command:?}");
    let output = deadline::output_within(role, &mut command, &deadline::start(role, ZFS))
        .map_err(|error| host(&described, error))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(host(
            format_args!("{described} ended {}", output.status),
            String::from_utf8_lossy(&output.stderr).trim_end(),
        ))
    }
}

/// An unprivileged `zfs` read. `zfs` is the one the helper runs
/// ([`cowshed_helper::host::TRUSTED_PATH`]): a daemon's `PATH` need not hold it.
fn read_zfs(arguments: &[&str]) -> Result<String, ApfsStorageError> {
    let mut command = Command::new("zfs");
    command
        .args(arguments)
        .env("PATH", cowshed_helper::host::TRUSTED_PATH);
    run("zfs read", command)
}

/// Whether this host has a `zfs` to ask.
fn zfs_installed() -> bool {
    std::env::split_paths(cowshed_helper::host::TRUSTED_PATH)
        .any(|directory| directory.join("zfs").is_file())
}

/// This process's real uid and the cowshed roots that name it as their owner.
fn roots_serving_this_user() -> Result<(u32, Vec<String>), ApfsStorageError> {
    // SAFETY: reads this process's real uid.
    let uid = unsafe { libc::getuid() };
    let property = crate::storage::bootstrap::ZFS_OWNER_PROPERTY;
    let listed = read_zfs(&[
        "list",
        "-H",
        "-o",
        &format!("name,{property}"),
        "-t",
        "filesystem",
    ])?;
    let roots = serving_roots(&listed, uid)
        .into_iter()
        .map(str::to_owned)
        .collect();
    Ok((uid, roots))
}

/// What `ask` answers of the directory at `place`; a place that does not exist holds nothing.
fn dataset_at(
    place: &Path,
    ask: impl FnOnce(&File) -> io::Result<Option<String>>,
) -> Result<Option<String>, ApfsStorageError> {
    let opened = match File::open(place) {
        Ok(opened) => opened,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("open payload directory", place, error)),
    };
    ask(&opened).map_err(|error| io_error("read the mount at", place, error))
}

/// The dataset ZFS is to mount at `place`: the one whose `mountpoint` property names it, asked
/// of ZFS when nothing is mounted there (a reboot or an import that did not mount).
fn assigned_to(place: &Path) -> Result<Option<String>, ApfsStorageError> {
    let listed = read_zfs(&["list", "-H", "-o", "name,mountpoint", "-t", "filesystem"])?;
    Ok(assigned_in(&listed, path_text(place)?).map(str::to_owned))
}

/// The name in `listed` (`zfs list -H -o name,mountpoint`) whose mountpoint is `place`.
fn assigned_in<'a>(listed: &'a str, place: &str) -> Option<&'a str> {
    listed
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .find_map(|(name, mountpoint)| (mountpoint == place).then_some(name))
}

/// The cowshed roots in `listed` (`zfs list -H -o name,cowshed:owner`) that name `uid`.
fn serving_roots(listed: &str, uid: u32) -> Vec<&str> {
    let uid = uid.to_string();
    listed
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .filter(|(name, owner)| *owner == uid && Root::parse(name).is_ok())
        .map(|(name, _)| name)
        .collect()
}

/// The cowshed root a main workspace's dataset, `<root>/projects/<owner>/<repo>/main`, lies in.
fn root_of_main(dataset: &str) -> Option<Root> {
    let mut parts = dataset.rsplitn(5, '/');
    match (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    ) {
        ("main", _repo, _owner, "projects", root) => Root::parse(root).ok(),
        _ => None,
    }
}

impl ZfsPayloads {
    /// The payloads of the store at `store_root`, when that directory is the mounted root of the
    /// `store` dataset of a cowshed root (`<parent>/cowshed/store`; 09_substrates.md, "Fixed
    /// dataset hierarchy"): the kernel's mount table names the root, and nothing else does. The
    /// helper is the one beside this executable or on `PATH` ([`choose_helper`]), resolved here
    /// because `sudo` does not search the caller's `PATH`.
    pub(super) fn of_store(store_root: &Path) -> Result<Option<Self>, ApfsStorageError> {
        Self::of_store_beside(store_root, beside_this_executable())
    }

    /// The payloads of one project, whatever filesystem holds the store. `main` is the store's
    /// entry for its main workspace, and that entry says what the project keeps:
    ///
    /// - A regular file is a disk image: no datasets.
    /// - Any other entry is a mount record or a home payload: the dataset mounted where it
    ///   says, or, with nothing mounted there, the one whose `mountpoint` names the place, is
    ///   `<root>/projects/<owner>/<repo>/main`, which names the root.
    /// - With no entry the project is not adopted yet and keeps what its adoption asks for:
    ///   datasets only when `adopt_datasets`, below the one cowshed root that names this user.
    pub(super) fn of_project(
        store_root: &Path,
        main: &Path,
        adopt_datasets: bool,
    ) -> Result<Option<Self>, ApfsStorageError> {
        let root = match fs::symlink_metadata(main) {
            Ok(metadata) if metadata.is_file() => return Ok(None),
            Ok(metadata) => {
                let place = if metadata.file_type().is_symlink() {
                    fs::read_link(main)
                        .map_err(|error| io_error("read mount record", main, error))?
                } else {
                    main.to_owned()
                };
                let dataset = match dataset_at(&place, cowshed_helper::host::mounted_dataset)? {
                    Some(mounted) => Some(mounted),
                    None => assigned_to(&place)?,
                };
                dataset.as_deref().and_then(root_of_main)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !adopt_datasets {
                    return Ok(None);
                }
                let property = crate::storage::bootstrap::ZFS_OWNER_PROPERTY;
                let (uid, roots) = roots_serving_this_user()?;
                match roots.as_slice() {
                    [root] => Root::parse(root).ok(),
                    [] => {
                        return Err(host(
                            format_args!("no cowshed ZFS root names uid {uid} as its owner"),
                            format_args!(
                                "whoever administers a pool creates one: zfs create -o mountpoint=none -o {property}={uid} <pool>/cowshed"
                            ),
                        ));
                    }
                    several => {
                        return Err(host(
                            format_args!("several cowshed ZFS roots name uid {uid} as their owner"),
                            several.join(", "),
                        ));
                    }
                }
            }
            Err(error) => return Err(io_error("inspect main payload", main, error)),
        };
        match root {
            Some(root) => Self::with_root(root, store_root, beside_this_executable()).map(Some),
            None => Ok(None),
        }
    }

    /// Every root whose datasets the cowshed installed in `beside` may come to change from the
    /// store at `store_root`: the store's own root when the store is a dataset, else each root
    /// that names this user, which a project is adopted onto by asking
    /// ([`Self::of_project`]). A host with no `zfs` has none.
    pub(super) fn serving(store_root: &Path, beside: &Path) -> Result<Vec<Self>, ApfsStorageError> {
        if let Some(own) = Self::of_store_beside(store_root, Some(beside.to_owned()))? {
            return Ok(vec![own]);
        }
        if !zfs_installed() {
            return Ok(Vec::new());
        }
        let (_, roots) = roots_serving_this_user()?;
        roots
            .iter()
            .filter_map(|root| Root::parse(root).ok())
            .map(|root| Self::with_root(root, store_root, Some(beside.to_owned())))
            .collect()
    }

    /// [`Self::of_store`] for the cowshed whose executable lies in `beside`: the payloads as that
    /// cowshed reaches them, whichever executable asks.
    pub(super) fn of_store_beside(
        store_root: &Path,
        beside: Option<PathBuf>,
    ) -> Result<Option<Self>, ApfsStorageError> {
        let opened = File::open(store_root)
            .map_err(|error| io_error("open store root", store_root, error))?;
        let mounted = cowshed_helper::host::mounted_dataset(&opened)
            .map_err(|error| io_error("read the mount at", store_root, error))?;
        let Some(root) = mounted
            .as_deref()
            .and_then(|dataset| dataset.strip_suffix("/store"))
            .and_then(|root| Root::parse(root).ok())
        else {
            return Ok(None);
        };
        Self::with_root(root, store_root, beside).map(Some)
    }

    fn with_root(
        root: Root,
        store_root: &Path,
        beside: Option<PathBuf>,
    ) -> Result<Self, ApfsStorageError> {
        let searched = std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>());
        let helper =
            choose_helper(helpers_in(beside.into_iter().chain(searched))).ok_or_else(|| {
                host(
                    format_args!("{} is a cowshed ZFS root", root.dataset().as_str()),
                    "cowshed-helper is installed neither beside cowshed nor on PATH",
                )
            })?;
        Ok(Self {
            root,
            store_root: store_root.to_owned(),
            helper,
        })
    }

    /// The cowshed root these payloads lie below.
    pub(super) fn root(&self) -> &Root {
        &self.root
    }

    /// The helper every mutation of these payloads goes through.
    pub(super) fn helper_path(&self) -> &Path {
        &self.helper
    }

    fn run(&self, role: &'static str, command: Command) -> Result<String, ApfsStorageError> {
        run(role, command)
    }

    /// An unprivileged `zfs` read.
    fn zfs(&self, arguments: &[&str]) -> Result<String, ApfsStorageError> {
        read_zfs(arguments)
    }

    /// The helper's command line up to its verb, elevated as the helper is installed. `sudo`
    /// never prompts (`-n`) and takes `sudo_flags` besides.
    fn elevated(&self, sudo_flags: &[&str]) -> Command {
        let installed = fs::metadata(&self.helper)
            .map(|metadata| elevation(metadata.uid(), metadata.permissions().mode()))
            .unwrap_or(Elevation::Sudo);
        let mut command = match installed {
            Elevation::Setuid => Command::new(&self.helper),
            Elevation::Sudo => {
                let mut sudo = Command::new("sudo");
                sudo.arg("-n").args(sudo_flags).arg(&self.helper);
                sudo
            }
        };
        command.arg(self.root.dataset().as_str());
        command
    }

    /// One helper verb on this root.
    fn helper(&self, arguments: &[&str]) -> Result<(), ApfsStorageError> {
        let mut command = self.elevated(&[]);
        command.args(arguments);
        self.run("cowshed-helper", command).map(drop)
    }

    /// Prove, changing nothing, that the helper acts as root on this root: its `check` verb
    /// answers only when it runs as root and the root's `cowshed:owner` names this user. Through
    /// `sudo` the credentials a terminal cached are set aside (`-k`), so only a standing sudoers
    /// entry passes: a daemon has no terminal a password was typed at.
    pub(super) fn prove_helper(&self) -> Result<(), ApfsStorageError> {
        let mut command = self.elevated(&["-k"]);
        command.arg("check");
        self.run("cowshed-helper check", command).map(drop)
    }

    fn exists(&self, name: &str) -> bool {
        self.zfs(&["list", "-H", "-o", "name", name]).is_ok()
    }

    /// One property of a dataset or snapshot, machine-readable.
    fn property(&self, name: &str, property: &str) -> Result<String, ApfsStorageError> {
        self.zfs(&["get", "-H", "-p", "-o", "value", property, name])
            .map(|value| value.trim_end().to_owned())
    }

    /// Make the container `name` when it does not exist: a dataset that never mounts and only
    /// holds others. Two creators may race; the loser finds it made.
    fn ensure_container(&self, name: &str) -> Result<(), ApfsStorageError> {
        if self.exists(name) {
            return Ok(());
        }
        match self.helper(&["create", name, "mountpoint=none", "canmount=off"]) {
            Err(_) if self.exists(name) => Ok(()),
            made => made,
        }
    }

    /// The dataset name of the payload at `image`,
    /// `<root>/projects/<owner>/<repo>/<place>` ([`place_name`]), its containers made first.
    fn named(&self, image: &Path) -> Result<String, ApfsStorageError> {
        let outside = || host("not a payload of a project in the store", image.display());
        let relative = image
            .strip_prefix(&self.store_root)
            .map_err(|_| outside())?;
        let mut parts = relative.components();
        let (Some(owner), Some(repo)) = (parts.next(), parts.next()) else {
            return Err(outside());
        };
        let component = |part: std::path::Component<'_>| {
            part.as_os_str()
                .to_str()
                .map(dataset_component)
                .ok_or_else(outside)
        };
        let mut place = place_name(parts.as_path()).ok_or_else(outside)?;
        let leaf = place.pop().ok_or_else(outside)?;
        let mut name = self.root.dataset().to_string();
        for part in ["projects".to_owned(), component(owner)?, component(repo)?]
            .into_iter()
            .chain(place)
        {
            name = format!("{name}/{part}");
            Dataset::parse(&name).map_err(|error| host(&name, error))?;
            self.ensure_container(&name)?;
        }
        let name = format!("{name}/{leaf}");
        Dataset::parse(&name).map_err(|error| host(&name, error))?;
        Ok(name)
    }

    /// The dataset mounted at `directory`, when it is one of this root's.
    fn mounted_at(&self, directory: &Path) -> Result<Option<Dataset>, ApfsStorageError> {
        let opened = match File::open(directory) {
            Ok(opened) => opened,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error("open payload directory", directory, error)),
        };
        let source = cowshed_helper::host::mounted_dataset(&opened)
            .map_err(|error| io_error("read the mount at", directory, error))?;
        Ok(source
            .and_then(|name| Dataset::parse(&name).ok())
            .filter(|dataset| self.root.holds(dataset)))
    }

    fn payload_at(&self, directory: &Path) -> Result<Dataset, ApfsStorageError> {
        self.mounted_at(directory)?.ok_or_else(|| {
            host(
                "no dataset of this cowshed root is mounted at",
                directory.display(),
            )
        })
    }

    /// Whether a payload's dataset is mounted at `directory`.
    pub(super) fn is_mounted(&self, directory: &Path) -> Result<bool, ApfsStorageError> {
        Ok(self.mounted_at(directory)?.is_some())
    }

    /// The cap of the payload mounted at `directory`: its dataset's `refquota` in bytes, which
    /// `statfs` on the mountpoint reports as the filesystem's size. `None` for a dataset an
    /// operator left uncapped.
    pub(super) fn capacity(&self, directory: &Path) -> Result<Option<u64>, ApfsStorageError> {
        let dataset = self.payload_at(directory)?;
        let quota = self.property(dataset.as_str(), "refquota")?;
        let bytes = quota
            .parse::<u64>()
            .map_err(|error| host(format_args!("refquota {quota:?} of {dataset}"), error))?;
        Ok((bytes != 0).then_some(bytes))
    }

    /// Cap the payload mounted at `directory` at `bytes`, in place: a dataset's `refquota`
    /// changes while it is mounted and in use. ZFS refuses a cap below what the dataset holds.
    pub(super) fn set_capacity(
        &self,
        directory: &Path,
        bytes: u64,
    ) -> Result<(), ApfsStorageError> {
        let dataset = self.payload_at(directory)?;
        self.helper(&["set", dataset.as_str(), &format!("refquota={bytes}")])
    }

    /// Make an empty payload at `directory`, which must not exist, capped at `refquota` bytes.
    /// The helper hands the new dataset's root to this user.
    pub(super) fn create(
        &self,
        directory: &Path,
        refquota: Option<u64>,
    ) -> Result<(), ApfsStorageError> {
        let name = self.named(directory)?;
        fs::create_dir(directory).map_err(|error| io_error("create payload", directory, error))?;
        let mountpoint = format!("mountpoint={}", path_text(directory)?);
        let quota = refquota.map(|bytes| format!("refquota={bytes}"));
        let mut arguments = vec!["create", name.as_str(), mountpoint.as_str()];
        arguments.extend(quota.as_deref());
        self.helper(&arguments).inspect_err(|_| {
            let _ = fs::remove_dir(directory);
        })
    }

    /// Clone the payload mounted at `source` to a new payload at `destination`, which must not
    /// exist: one snapshot of the source, transactionally atomic, and one clone of it
    /// (09_substrates.md, "The clone-origin dependency"). The clone keeps the source's
    /// `refquota`, which a clone does not inherit. A failed clone leaves neither.
    pub(super) fn clone_to(
        &self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), ApfsStorageError> {
        let origin = self.payload_at(source)?;
        let name = self.named(destination)?;
        // One origin snapshot per clone, named for it (09_substrates.md, "The clone-origin
        // dependency").
        let fork = name.rsplit('/').next().unwrap_or_default();
        let snapshot = format!("{origin}@cowshed:fork-{fork}");
        let quota = self.property(origin.as_str(), "refquota")?;
        fs::create_dir(destination)
            .map_err(|error| io_error("create clone payload", destination, error))?;
        let mountpoint = format!("mountpoint={}", path_text(destination)?);
        let quota = (quota != "0").then(|| format!("refquota={quota}"));
        let mut arguments = vec![
            "clone",
            snapshot.as_str(),
            name.as_str(),
            mountpoint.as_str(),
        ];
        arguments.extend(quota.as_deref());
        let cloned = self
            .helper(&["snapshot", &snapshot])
            .and_then(|()| self.helper(&arguments));
        cloned.inspect_err(|_| {
            let _ = self.helper(&["destroy", &snapshot]);
            let _ = fs::remove_dir(destination);
        })
    }

    /// Move the payload mounted at `from` to `to`, an empty directory or no entry: the dataset
    /// remounts there, and `from` is left the empty directory an unmounted mountpoint is. `force`
    /// unmounts out from under whoever holds the tree open; without it a held payload refuses.
    pub(super) fn relocate(
        &self,
        from: &Path,
        to: &Path,
        force: bool,
    ) -> Result<(), ApfsStorageError> {
        let dataset = self.payload_at(from)?;
        match fs::create_dir(to) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error("create mountpoint", to, error)),
        }
        if force {
            self.helper(&["unmount", dataset.as_str(), "--force"])?;
        }
        let mountpoint = format!("mountpoint={}", path_text(to)?);
        self.helper(&["set", dataset.as_str(), &mountpoint])?;
        // Setting the property remounts a mounted dataset and leaves an unmounted one unmounted.
        if !self.is_mounted(to)? {
            self.helper(&["mount", dataset.as_str()])?;
        }
        Ok(())
    }

    /// The dataset of this root whose `mountpoint` is `directory`, mounted or not.
    fn assigned_to(&self, directory: &Path) -> Result<Option<String>, ApfsStorageError> {
        let wanted = path_text(directory)?;
        let listed = self.zfs(&[
            "list",
            "-H",
            "-p",
            "-r",
            "-t",
            "filesystem",
            "-o",
            "name,mountpoint",
            self.root.dataset().as_str(),
        ])?;
        Ok(listed
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .find_map(|(name, mountpoint)| (mountpoint == wanted).then(|| name.to_owned())))
    }

    /// The dataset that lost its mount at `directory`: the one of this root whose `mountpoint`
    /// is `directory` while nothing of this root is mounted there, as a pool import that mounted
    /// nothing or a `zfs unmount` leaves it. `None` while a payload is mounted there, and when no
    /// dataset has that mountpoint. A read: nothing is mounted.
    pub(super) fn lost_at(&self, directory: &Path) -> Result<Option<String>, ApfsStorageError> {
        if self.is_mounted(directory)? {
            return Ok(None);
        }
        self.assigned_to(directory)
    }

    /// Mount the dataset whose `mountpoint` is `directory`, when one of this root's has it and
    /// is not mounted: a mount lost to a reboot or a pool import that did not mount. Answers
    /// whether a payload is mounted there afterwards.
    pub(super) fn remount(&self, directory: &Path) -> Result<bool, ApfsStorageError> {
        if self.is_mounted(directory)? {
            return Ok(true);
        }
        let Some(dataset) = self.assigned_to(directory)? else {
            return Ok(false);
        };
        self.helper(&["mount", &dataset])?;
        self.is_mounted(directory)
    }

    /// The snapshots of `dataset`, each with the clones that depend on it.
    fn snapshots(&self, dataset: &Dataset) -> Result<Vec<(String, Vec<String>)>, ApfsStorageError> {
        let listed = self.zfs(&[
            "list",
            "-H",
            "-p",
            "-d",
            "1",
            "-t",
            "snapshot",
            "-o",
            "name,clones",
            dataset.as_str(),
        ])?;
        Ok(listed
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .map(|(name, clones)| {
                let clones = clones
                    .split(',')
                    .filter(|clone| !clone.is_empty() && *clone != "-")
                    .map(str::to_owned)
                    .collect();
                (name.to_owned(), clones)
            })
            .collect())
    }

    /// The datasets cloned from a snapshot of the payload mounted at `directory`: its forks.
    pub(super) fn dependents(&self, directory: &Path) -> Result<Vec<String>, ApfsStorageError> {
        let dataset = self.payload_at(directory)?;
        Ok(self
            .snapshots(&dataset)?
            .into_iter()
            .flat_map(|(_, clones)| clones)
            .collect())
    }

    /// Destroy the payload mounted at `directory` and remove the directory: its dataset, the
    /// snapshots it holds and then its origin snapshot, clone before origin (09_substrates.md).
    /// A build dataset whose snapshot is another's origin first hands its lineage to that clone
    /// by `zfs promote` (01b_zfs.md, "Retiring a build dataset"). A workspace dataset is never
    /// promoted over, so one that still has a fork is kept, mounted as it was, and the answer is
    /// `None`; otherwise it is the bytes the dataset held (`used`), read before it went.
    pub(super) fn destroy(&self, directory: &Path) -> Result<Option<u64>, ApfsStorageError> {
        let dataset = self.payload_at(directory)?;
        // Any dataset in the project's `build` container, a released one in `build/.deleting`
        // included.
        let build = dataset
            .as_str()
            .strip_prefix(self.root.dataset().as_str())
            .is_some_and(|below| below.split('/').nth(4) == Some("build"));
        if build {
            // Each promote takes the promoted clone's origin and every older snapshot with it, so
            // one pass per remaining pinned snapshot ends with none.
            while let Some(clone) = self
                .snapshots(&dataset)?
                .into_iter()
                .find_map(|(_, clones)| clones.into_iter().next())
            {
                self.helper(&["promote", &clone])?;
            }
        }
        let snapshots = self.snapshots(&dataset)?;
        if snapshots.iter().any(|(_, clones)| !clones.is_empty()) {
            return Ok(None);
        }
        // Read after any promote: a promoted-over dataset is now a clone of its former clone.
        let origin = self.property(dataset.as_str(), "origin")?;
        let used = self.property(dataset.as_str(), "used")?;
        let used = used
            .parse()
            .map_err(|error| host(format_args!("`used` of {dataset} is {used:?}"), error))?;
        self.helper(&["unmount", dataset.as_str(), "--force"])?;
        for (snapshot, _) in &snapshots {
            self.helper(&["destroy", snapshot])?;
        }
        self.helper(&["destroy", dataset.as_str()])?;
        if origin != "-" {
            // Deferred: an origin another clone still depends on goes with its last clone.
            self.helper(&["destroy", &origin, "--defer"])?;
        }
        fs::remove_dir(directory).map_err(|error| {
            io_error("remove a destroyed payload's directory", directory, error)
        })?;
        Ok(Some(used))
    }
}

/// The store's mount protocol over datasets. The record is the directory store's: while a
/// payload is mounted its image path is a symlink naming the mountpoint, and while it is home
/// the image path is the directory its dataset is mounted at. A move is three steps: the new
/// record is written beside the image path (its move sibling), the dataset's `mountpoint` is set,
/// and the record takes the image path's place. The kernel's mount table decides a move a crash
/// interrupted: wherever the dataset is mounted is where the payload is.
impl ZfsPayloads {
    /// Where `image`'s payload is, after completing a move a crash interrupted and mounting a
    /// dataset a reboot left unmounted.
    pub(super) fn settle(&self, image: &Path) -> Result<Payload, ApfsStorageError> {
        let sibling = move_sibling(image);
        let metadata = match fs::symlink_metadata(image) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(io_error("inspect payload", image, error)),
        };
        if let Some(metadata) = &metadata
            && metadata.file_type().is_symlink()
        {
            let mount_point = fs::read_link(image)
                .map_err(|error| io_error("read mount record", image, error))?;
            if !mount_point.is_absolute() {
                return Err(host(
                    "a mount record names a relative path",
                    image.display(),
                ));
            }
            return if self.remount(&mount_point)? {
                Ok(Payload::Mounted(mount_point))
            } else {
                Err(host(
                    format_args!(
                        "mount record {} names a mountpoint no dataset has",
                        image.display()
                    ),
                    mount_point.display(),
                ))
            };
        }
        if metadata.as_ref().is_some_and(|metadata| !metadata.is_dir()) {
            return Err(host(
                "payload is neither a dataset's directory nor a mount record",
                image.display(),
            ));
        }
        if metadata.is_some() && self.is_mounted(image)? {
            if mount_point_state(&sibling)? == MountPointState::Link {
                clear_move_sibling(&sibling)?;
            }
            return Ok(Payload::Home);
        }
        // No payload at the image path. A record waiting beside it names where a move in flight
        // was taking it, or bringing it back from.
        if mount_point_state(&sibling)? == MountPointState::Link {
            let mount_point = fs::read_link(&sibling)
                .map_err(|error| io_error("read mount record", &sibling, error))?;
            if self.remount(&mount_point)? {
                if metadata.is_some() {
                    fs::remove_dir(image)
                        .map_err(|error| io_error("clear a moved payload's home", image, error))?;
                }
                fs::rename(&sibling, image)
                    .map_err(|error| io_error("record mount", image, error))?;
                sync_parent_path(image)?;
                return Ok(Payload::Mounted(mount_point));
            }
        }
        if metadata.is_some() && self.remount(image)? {
            if mount_point_state(&sibling)? == MountPointState::Link {
                clear_move_sibling(&sibling)?;
            }
            return Ok(Payload::Home);
        }
        Err(host(
            "no dataset is mounted or mountable at",
            image.display(),
        ))
    }

    /// Mount the home payload `image` at `mount_point`, a vacant or absent path the caller
    /// checked.
    pub(super) fn bind(&self, image: &Path, mount_point: &Path) -> Result<(), ApfsStorageError> {
        let sibling = move_sibling(image);
        clear_move_sibling(&sibling)?;
        std::os::unix::fs::symlink(mount_point, &sibling)
            .map_err(|error| io_error("create mount record", &sibling, error))?;
        if let Err(primary) = self.relocate(image, mount_point, false) {
            return combine_cleanup(
                "mount payload",
                primary,
                fs::remove_file(&sibling)
                    .map_err(|error| io_error("remove unused mount record", &sibling, error)),
            );
        }
        fs::remove_dir(image)
            .map_err(|error| io_error("clear a mounted payload's home", image, error))?;
        fs::rename(&sibling, image).map_err(|error| io_error("record mount", image, error))?;
        sync_parent_path(image)
    }

    /// Bring the payload mounted at `mount_point` home to `image` and leave the empty directory
    /// an unmounted mountpoint is.
    pub(super) fn move_home(
        &self,
        image: &Path,
        mount_point: &Path,
        force: bool,
    ) -> Result<(), ApfsStorageError> {
        let sibling = move_sibling(image);
        clear_move_sibling(&sibling)?;
        fs::rename(image, &sibling)
            .map_err(|error| io_error("set a mount record aside", image, error))?;
        if let Err(primary) = self.relocate(mount_point, image, force) {
            let rollback = match fs::remove_dir(image) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => {
                    Err(io_error("clear an unused home", image, error))
                }
                _ => fs::rename(&sibling, image)
                    .map_err(|error| io_error("restore mount record", image, error)),
            };
            return combine_cleanup("unmount payload", primary, rollback);
        }
        fs::remove_file(&sibling)
            .map_err(|error| io_error("remove released mount record", &sibling, error))?;
        sync_parent_path(image)
    }

    /// Move the home payload at `from` to `to`, which must not exist: what a rename is to a
    /// directory payload. The dataset takes the name of its new place first, then mounts there.
    pub(super) fn rename(&self, from: &Path, to: &Path) -> Result<(), ApfsStorageError> {
        if fs::symlink_metadata(to).is_ok() {
            return Err(host("a payload's new place already exists", to.display()));
        }
        let dataset = self.payload_at(from)?;
        let renamed = self.named(to)?;
        if dataset.as_str() != renamed {
            self.helper(&["rename", dataset.as_str(), &renamed])?;
            // Its origin snapshot is named for the fork, so it follows the fork's name and
            // leaves the old one free for the next fork of that name.
            let leaf = |name: &str| name.rsplit('/').next().unwrap_or_default().to_owned();
            let origin = self.property(&renamed, "origin")?;
            if let Some((source, snapshot)) = origin.split_once('@')
                && snapshot == format!("cowshed:fork-{}", leaf(dataset.as_str()))
            {
                let followed = format!("{source}@cowshed:fork-{}", leaf(&renamed));
                self.helper(&["rename", &origin, &followed])?;
            }
        }
        self.relocate(from, to, false)?;
        fs::remove_dir(from).map_err(|error| io_error("clear a moved payload's place", from, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A project's main dataset names its cowshed root, and only a main dataset does; a lost
    /// mount is found by the exact mountpoint ZFS holds for it.
    #[test]
    fn a_projects_main_dataset_names_its_root() {
        for (dataset, root) in [
            (
                "tank/cowshed/projects/acme/widget/main",
                Some("tank/cowshed"),
            ),
            (
                "tank/a/b/cowshed/projects/acme/widget/main",
                Some("tank/a/b/cowshed"),
            ),
            ("tank/cowshed/projects/acme/widget/ws/raven", None),
            ("tank/cowshed/projects/acme/widget", None),
            ("tank/other/projects/acme/widget/main", None),
            ("tank/cowshed/store", None),
            ("main", None),
        ] {
            assert_eq!(
                root_of_main(dataset).map(|root| root.dataset().as_str().to_owned()),
                root.map(str::to_owned),
                "{dataset}"
            );
        }
        let listed = "tank/cowshed\tnone\n\
                      tank/cowshed/projects/acme/widget/main\t/Users/u/widget\n\
                      tank/cowshed/projects/acme/widget/ws/raven\t/Users/u/widget-raven\n";
        assert_eq!(
            assigned_in(listed, "/Users/u/widget"),
            Some("tank/cowshed/projects/acme/widget/main")
        );
        assert_eq!(assigned_in(listed, "/Users/u"), None);
        assert_eq!(assigned_in(listed, "none"), Some("tank/cowshed"));
        let owners = "tank/cowshed\t501\ntank/x/cowshed\t502\ntank/y/cowshed\t-\n\
                      tank/cowshed/store\t501\ntank/z\t501\n";
        assert_eq!(serving_roots(owners, 501), ["tank/cowshed"]);
        assert_eq!(serving_roots(owners, 500), [""; 0]);
    }

    #[test]
    fn the_helper_elevates_itself_only_when_it_is_setuid_root() {
        for (owner, mode, expected) in [
            (0, 0o104755, Elevation::Setuid),
            (0, 0o100755, Elevation::Sudo),
            (1000, 0o104755, Elevation::Sudo),
            (1000, 0o100755, Elevation::Sudo),
        ] {
            assert_eq!(elevation(owner, mode), expected, "{owner} {mode:o}");
        }
    }

    #[test]
    fn a_helper_that_elevates_itself_serves_before_one_sudo_must_admit() {
        use Elevation::{Setuid, Sudo};
        type Case<'a> = (&'a [(&'a str, Elevation)], Option<&'a str>);
        let cases: [Case<'_>; 6] = [
            (&[], None),
            (&[("beside", Sudo)], Some("beside")),
            (&[("beside", Setuid)], Some("beside")),
            // An administrator's setuid install on `PATH` needs no sudoers entry, so it serves
            // although the user's own install comes first.
            (&[("beside", Sudo), ("wrappers", Setuid)], Some("wrappers")),
            (&[("beside", Sudo), ("usr", Sudo)], Some("beside")),
            (
                &[("beside", Sudo), ("wrappers", Setuid), ("usr", Setuid)],
                Some("wrappers"),
            ),
        ];
        for (candidates, expected) in cases {
            assert_eq!(
                choose_helper(candidates.iter().copied()),
                expected,
                "{candidates:?}"
            );
        }
    }

    #[test]
    fn a_dataset_is_named_for_its_payloads_place_in_the_project() {
        for (relative, expected) in [
            ("main.dmg", Some("main")),
            ("sessions/raven.dmg", Some("ws/raven")),
            ("sessions/.trash/raven-1f.dmg", Some("ws/.trash/raven-1f")),
            (
                "sessions/.deleting/reclaimed-2a.dmg",
                Some("ws/.deleting/reclaimed-2a"),
            ),
            (".staging/main.dmg", Some(".staging/main")),
            ("build/7.dmg", Some("build/7")),
            ("build/.deleting/7.dmg", Some("build/.deleting/7")),
            ("sessions/raven", None),
            ("sessions/.dmg", None),
            ("", None),
        ] {
            assert_eq!(
                place_name(Path::new(relative))
                    .map(|parts| parts.join("/"))
                    .as_deref(),
                expected,
                "{relative}"
            );
        }
        assert_eq!(dataset_component("acme%2Fwidget"), "acme:2Fwidget");
    }
}
