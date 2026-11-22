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

/// The helper that serves, of the helper files there are in search order (the root-owned
/// system directory, beside cowshed, then each `PATH` directory) with how each gets its
/// privilege: the first that elevates itself, an administrator's install that needs nothing
/// more, else the first there is, which `sudo` must admit (09_substrates.md, "Privileged
/// helper").
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

/// Whether `name`, a dataset or snapshot listed below the project container `project`, is one of
/// the project's containers: the project's own, `ws` and `build`, and the `.`-named ones inside
/// those (`.trash`, `.deleting`), which no payload is named like ([`place_name`]: a workspace or
/// build volume never starts with `.`). Snapshots and every payload are not.
fn is_project_container(project: &str, name: &str) -> bool {
    if name.contains('@') {
        return false;
    }
    let Some(below) = name.strip_prefix(project) else {
        return false;
    };
    match below.split('/').collect::<Vec<_>>().as_slice() {
        [""] => true,
        ["", "ws" | "build"] => true,
        ["", "ws" | "build", inner] => inner.starts_with('.'),
        _ => false,
    }
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

/// The owner and repository of the store's project whose payload `image` is, each as a dataset
/// name component, and the payload's place in that project: `<store>/<owner>/<repo>/<place>`.
fn project_of<'a>(store_root: &Path, image: &'a Path) -> Option<(String, String, &'a Path)> {
    let mut parts = image.strip_prefix(store_root).ok()?.components();
    let mut component = || parts.next()?.as_os_str().to_str().map(dataset_component);
    let (owner, repo) = (component()?, component()?);
    Some((owner, repo, parts.as_path()))
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

/// Where a payload is while it is home (01b_zfs.md, "One pool, one root").
#[derive(Clone, Debug)]
enum Home {
    /// The store is the root's `store` dataset: a payload at home is mounted at its image path.
    InStore,
    /// The store is not a dataset, as macOS's APFS store is not, and its volume ignores
    /// ownership: a payload at home is never mounted (`mountpoint=none`, `canmount=noauto`), its
    /// image path is an empty directory that names it by place, and it mounts only at its bind
    /// place. `scratch` is the directory of this user's where a new dataset is mounted once, so
    /// the helper hands its root to this user, before it goes home.
    Unmounted { scratch: PathBuf },
}

/// The datasets of one cowshed root, as payloads of the store at `store_root`.
#[derive(Clone, Debug)]
pub struct ZfsPayloads {
    root: Root,
    store_root: PathBuf,
    helper: PathBuf,
    home: Home,
}

/// The directory of this executable, through the links that name it: the helper is installed
/// beside the executable itself.
fn beside_this_executable() -> Option<PathBuf> {
    std::env::current_exe()
        .and_then(fs::canonicalize)
        .ok()
        .and_then(|executable| Some(executable.parent()?.to_owned()))
}

/// The root helper beside this executable ([`super::build_root_helper`]).
pub(super) fn build_root_helper() -> Option<PathBuf> {
    Some(beside_this_executable()?.join(HELPER)).filter(|helper| helper.is_file())
}

/// [`super::roots_awaiting_root_helper`].
pub(super) fn roots_awaiting_root_helper(
    sha256: &str,
    created: &[String],
) -> Result<Vec<String>, ApfsStorageError> {
    if !zfs_installed() {
        return Ok(Vec::new());
    }
    let (_, roots) = roots_serving_this_user()?;
    if roots.is_empty() && created.is_empty() {
        return Ok(Vec::new());
    }
    let system = Path::new(cowshed_helper::host::SYSTEM_DIRECTORY).join(HELPER);
    let current = match super::file_sha256(&system) {
        Ok(digest) => digest == sha256,
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(io_error("hash the system root helper", &system, error)),
    };
    let mut awaiting = roots
        .into_iter()
        .filter(|root| {
            if !current {
                return true;
            }
            let mut command = Command::new("sudo");
            command
                .args(["-n", "-k"])
                .arg(&system)
                .args([root.dataset().as_str(), "check"]);
            run("cowshed-helper check", command).is_err()
        })
        .map(|root| root.dataset().to_string())
        .collect::<Vec<_>>();
    if !current {
        awaiting.extend(created.iter().cloned());
    }
    Ok(awaiting)
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

/// [`super::root_evidence`]: one `zfs get` of the root's owner and, when `apple`, its four
/// com.apple properties. Only `zfs` saying the dataset does not exist reads as
/// [`RootEvidence::Absent`](crate::storage::bootstrap::zfs_root::RootEvidence::Absent), or as
/// [`RootEvidence::Orphan`](crate::storage::bootstrap::zfs_root::RootEvidence::Orphan) when it
/// says so of the root's parent too; every other failure is the read's error.
pub(super) fn root_evidence(
    root: &Root,
    apple: bool,
) -> Result<crate::storage::bootstrap::zfs_root::RootEvidence, ApfsStorageError> {
    use crate::storage::bootstrap::zfs_root::{ROOT_APPLE_PROPERTIES, Read, RootEvidence};
    let owner = crate::storage::bootstrap::ZFS_OWNER_PROPERTY;
    let mut properties = vec![owner];
    if apple {
        properties.extend(ROOT_APPLE_PROPERTIES);
    }
    let mut command = Command::new("zfs");
    command
        .args(["get", "-H", "-p", "-o", "property,value,source"])
        .arg(properties.join(","))
        .arg(root.dataset().as_str())
        .env("PATH", cowshed_helper::host::TRUSTED_PATH)
        .env("LC_ALL", "C");
    let described = format!("{command:?}");
    let output =
        deadline::output_within("zfs read", &mut command, &deadline::start("zfs read", ZFS))
            .map_err(|error| host(&described, error))?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        if !stderr.contains("dataset does not exist") {
            return Err(host(
                format_args!("{described} ended {}", output.status),
                stderr.trim_end(),
            ));
        }
        let parent = root
            .dataset()
            .as_str()
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent);
        let mut command = Command::new("zfs");
        command
            .args(["list", "-H", "-o", "name", parent])
            .env("PATH", cowshed_helper::host::TRUSTED_PATH)
            .env("LC_ALL", "C");
        let described = format!("{command:?}");
        let output =
            deadline::output_within("zfs read", &mut command, &deadline::start("zfs read", ZFS))
                .map_err(|error| host(&described, error))?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        return if output.status.success() {
            Ok(RootEvidence::Absent)
        } else if stderr.contains("dataset does not exist") {
            Ok(RootEvidence::Orphan)
        } else {
            Err(host(
                format_args!("{described} ended {}", output.status),
                stderr.trim_end(),
            ))
        };
    }
    let listed = String::from_utf8_lossy(&output.stdout);
    // An unset user property reads `-` with source `-`: none.
    let read = |property: &str| {
        listed
            .lines()
            .filter_map(|line| {
                let mut fields = line.splitn(3, '\t');
                Some((fields.next()?, fields.next()?, fields.next()?))
            })
            .find(|(name, _, _)| *name == property)
            .filter(|(_, value, source)| (*value, *source) != ("-", "-"))
            .map(|(_, value, source)| Read {
                value: value.to_owned(),
                source: source.to_owned(),
            })
    };
    Ok(RootEvidence::Present {
        owner: read(owner),
        apple: ROOT_APPLE_PROPERTIES.map(|property| if apple { read(property) } else { None }),
    })
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
pub(super) fn zfs_installed() -> bool {
    std::env::split_paths(cowshed_helper::host::TRUSTED_PATH)
        .any(|directory| directory.join("zfs").is_file())
}

/// This process's real uid and the cowshed roots that name it as their owner.
pub(super) fn roots_serving_this_user() -> Result<(u32, Vec<Root>), ApfsStorageError> {
    // SAFETY: reads this process's real uid.
    let uid = unsafe { libc::getuid() };
    let property = crate::storage::bootstrap::ZFS_OWNER_PROPERTY;
    let listed = read_zfs(&[
        "get",
        "-H",
        "-t",
        "filesystem",
        "-o",
        "name,value,source",
        property,
    ])?;
    Ok((uid, serving_roots(&listed, uid)))
}

/// This process's real uid and the cowshed roots that name it and hold the main dataset of the
/// project whose main entry in the store is `main`, `<root>/projects/<owner>/<repo>/main`, by
/// name. A host with no `zfs` has none.
fn roots_holding(store_root: &Path, main: &Path) -> Result<(u32, Vec<Root>), ApfsStorageError> {
    let (owner, repo, _) = project_of(store_root, main)
        .ok_or_else(|| host("not a payload of a project in the store", main.display()))?;
    if !zfs_installed() {
        // SAFETY: reads this process's real uid.
        return Ok((unsafe { libc::getuid() }, Vec::new()));
    }
    let (uid, roots) = roots_serving_this_user()?;
    let holds = |root: &Root| {
        let name = format!("{}/projects/{owner}/{repo}/main", root.dataset());
        read_zfs(&["list", "-H", "-o", "name", &name]).is_ok()
    };
    Ok((uid, roots.into_iter().filter(holds).collect()))
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

/// The cowshed roots in `listed` (`zfs get -H -o name,value,source cowshed:owner`) whose owner is
/// set on them, locally, to `uid`: the property on the root itself is the whole authorization, and
/// an inherited or received one names no root (09_substrates.md, "Privileged helper").
fn serving_roots(listed: &str, uid: u32) -> Vec<Root> {
    let uid = uid.to_string();
    listed
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            Some((fields.next()?, fields.next()?, fields.next()?))
        })
        .filter(|(_, owner, source)| *owner == uid && *source == "local")
        .filter_map(|(name, _, _)| Root::parse(name).ok())
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

/// What a project's entry for main in the store says (09_substrates.md, selection), as the store
/// reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
enum MainEntry {
    /// A regular file: a disk image.
    File,
    /// A place a dataset is mounted at, or that a dataset's `mountpoint` names while nothing is
    /// mounted there: that dataset.
    Place(String),
    /// A place no dataset is mounted at or assigned to: a main at home, its dataset unmounted.
    Home,
    /// No entry: the project is not adopted yet.
    Absent,
}

/// Why no single cowshed root serves a project's datasets.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Unserved {
    /// Adoption asks for datasets and no root names the user.
    NoRoot { uid: u32 },
    /// More than one root would serve: cowshed never picks one.
    Several { uid: u32, roots: Vec<Root> },
    /// The root the project's `.cowshed.toml` declares does not name the user: cowshed never
    /// serves another's root, nor falls back to one it was not told.
    DeclaredNotServing { root: Root, uid: u32 },
}

impl From<Unserved> for ApfsStorageError {
    fn from(unserved: Unserved) -> Self {
        match unserved {
            Unserved::NoRoot { uid } => {
                let property = crate::storage::bootstrap::ZFS_OWNER_PROPERTY;
                host(
                    format_args!("no cowshed ZFS root names uid {uid} as its owner"),
                    format_args!(
                        "whoever administers a pool creates one: zfs create -o mountpoint=none -o {property}={uid} <pool>/cowshed"
                    ),
                )
            }
            Unserved::Several { uid, roots } => host(
                format_args!("several cowshed ZFS roots name uid {uid} as their owner"),
                roots
                    .iter()
                    .map(|root| root.dataset().as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            Unserved::DeclaredNotServing { root, uid } => {
                let property = crate::storage::bootstrap::ZFS_OWNER_PROPERTY;
                host(
                    format_args!(
                        "the cowshed ZFS root {} that .cowshed.toml declares does not name uid {uid} as its owner",
                        root.dataset()
                    ),
                    format_args!(
                        "cowshed setup creates it where it is absent; whoever administers its parent sets an existing one: zfs set {property}={uid} {}",
                        root.dataset()
                    ),
                )
            }
        }
    }
}

/// The one root of `roots`, `None` for none, refused for several.
fn the_one(uid: u32, roots: Vec<Root>) -> Result<Option<Root>, Unserved> {
    let mut roots = roots.into_iter();
    match (roots.next(), roots.next()) {
        (None, _) => Ok(None),
        (Some(root), None) => Ok(Some(root)),
        (Some(first), Some(second)) => Err(Unserved::Several {
            uid,
            roots: [first, second].into_iter().chain(roots).collect(),
        }),
    }
}

/// The cowshed root a project's datasets lie below, or `None` for a project that keeps disk
/// images (09_substrates.md, selection; proven in specs/cowshed/laws/zfs_substrate.bend). A
/// regular file is a disk image. A mounted or assigned dataset names its root
/// ([`root_of_main`]). A main at home is kept on the one serving root that holds its main dataset
/// by name, and keeps no datasets when none does. An absent main is not adopted yet: it keeps
/// datasets only when `adopt_datasets`: below the root the project declares (`declared`) when it
/// is among the roots that name the user, refused when it is not, and with nothing declared below
/// the one root that names the user. `serving` reads the roots that name the user and `holding`
/// those of them that hold the project's main dataset, each only for the entry that needs it.
fn select<E: From<Unserved>>(
    main: MainEntry,
    adopt_datasets: bool,
    declared: Option<Root>,
    serving: impl FnOnce() -> Result<(u32, Vec<Root>), E>,
    holding: impl FnOnce() -> Result<(u32, Vec<Root>), E>,
) -> Result<Option<Root>, E> {
    match main {
        MainEntry::File => Ok(None),
        MainEntry::Place(dataset) => Ok(root_of_main(&dataset)),
        MainEntry::Home => {
            let (uid, roots) = holding()?;
            Ok(the_one(uid, roots)?)
        }
        MainEntry::Absent if adopt_datasets => {
            let (uid, roots) = serving()?;
            match declared {
                Some(root) if roots.contains(&root) => Ok(Some(root)),
                Some(root) => Err(Unserved::DeclaredNotServing { root, uid }.into()),
                None => Ok(Some(the_one(uid, roots)?.ok_or(Unserved::NoRoot { uid })?)),
            }
        }
        MainEntry::Absent => Ok(None),
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
    ///   `<root>/projects/<owner>/<repo>/main`, which names the root. With neither, the main is
    ///   at home and its dataset unmounted: the one root that names this user and holds that
    ///   dataset by name is the project's.
    /// - With no entry the project is not adopted yet and keeps what its adoption asks for:
    ///   datasets only when `adopt_datasets`, below the root `declared` names (the parent the
    ///   project's `.cowshed.toml` declares) when that root names this user, else below the one
    ///   cowshed root that names this user.
    ///
    /// The choice is [`select`]'s; this reads what it needs.
    pub(super) fn of_project(
        store_root: &Path,
        main: &Path,
        adopt_datasets: bool,
        scratch: &Path,
        declared: Option<&str>,
    ) -> Result<Option<Self>, ApfsStorageError> {
        let entry = match fs::symlink_metadata(main) {
            Ok(metadata) if metadata.is_file() => MainEntry::File,
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
                dataset.map_or(MainEntry::Home, MainEntry::Place)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => MainEntry::Absent,
            Err(error) => return Err(io_error("inspect main payload", main, error)),
        };
        let declared = crate::storage::bootstrap::zfs_root::named_parent(None, declared)
            .map_err(|error| host(format_args!("the project's .cowshed.toml"), error))?;
        let root = select(
            entry,
            adopt_datasets,
            declared,
            roots_serving_this_user,
            || roots_holding(store_root, main),
        )?;
        match root {
            Some(root) => Self::with_root(root, store_root, beside_this_executable())
                .map(|payloads| Some(payloads.unmounted_at_home(scratch))),
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
            .into_iter()
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
        let system = PathBuf::from(cowshed_helper::host::SYSTEM_DIRECTORY);
        let helper = choose_helper(helpers_in(
            std::iter::once(system).chain(beside).chain(searched),
        ))
        .ok_or_else(|| {
            host(
                format_args!("{} is a cowshed ZFS root", root.dataset().as_str()),
                "cowshed-helper is installed neither by cowshed setup, beside cowshed nor on PATH",
            )
        })?;
        Ok(Self {
            root,
            store_root: store_root.to_owned(),
            helper,
            home: Home::InStore,
        })
    }

    /// These payloads, kept unmounted at home: the store is not a dataset. `scratch` is where
    /// each new dataset is mounted once to be handed to this user ([`Home::Unmounted`]).
    fn unmounted_at_home(self, scratch: &Path) -> Self {
        Self {
            home: Home::Unmounted {
                scratch: scratch.to_owned(),
            },
            ..self
        }
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

    /// The dataset name of the payload at `image`, `<root>/projects/<owner>/<repo>/<place>`
    /// ([`place_name`]), with the names of the containers above it, outermost first.
    fn placed(&self, image: &Path) -> Result<(Vec<String>, String), ApfsStorageError> {
        let outside = || host("not a payload of a project in the store", image.display());
        let (owner, repo, rest) = project_of(&self.store_root, image).ok_or_else(outside)?;
        let mut place = place_name(rest).ok_or_else(outside)?;
        let leaf = place.pop().ok_or_else(outside)?;
        let mut containers = Vec::new();
        let mut name = self.root.dataset().to_string();
        for part in ["projects".to_owned(), owner, repo]
            .into_iter()
            .chain(place)
        {
            name = format!("{name}/{part}");
            Dataset::parse(&name).map_err(|error| host(&name, error))?;
            containers.push(name.clone());
        }
        let name = format!("{name}/{leaf}");
        Dataset::parse(&name).map_err(|error| host(&name, error))?;
        Ok((containers, name))
    }

    /// The dataset name of the payload at `image` ([`Self::placed`]), its containers made first.
    fn named(&self, image: &Path) -> Result<String, ApfsStorageError> {
        let (containers, name) = self.placed(image)?;
        for container in &containers {
            self.ensure_container(container)?;
        }
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

    /// The payload at `directory`: the dataset of this root mounted there, or, for a payload at
    /// home in a store that is not a dataset, the one its image path names when it exists.
    fn payload_at(&self, directory: &Path) -> Result<Dataset, ApfsStorageError> {
        if let Some(mounted) = self.mounted_at(directory)? {
            return Ok(mounted);
        }
        if let Some(home) = self.at_home(directory)? {
            return Ok(home);
        }
        Err(host(
            "no dataset of this cowshed root is mounted at",
            directory.display(),
        ))
    }

    /// The dataset the image path `image` names, when the store keeps payloads unmounted at
    /// home, `image` is a directory and that dataset exists and is not mounted.
    fn at_home(&self, image: &Path) -> Result<Option<Dataset>, ApfsStorageError> {
        if !matches!(self.home, Home::Unmounted { .. }) || !image.starts_with(&self.store_root) {
            return Ok(None);
        }
        if !fs::symlink_metadata(image).is_ok_and(|metadata| metadata.is_dir()) {
            return Ok(None);
        }
        let (_, name) = self.placed(image)?;
        if !self.exists(&name) || self.property(&name, "mounted")? == "yes" {
            return Ok(None);
        }
        Dataset::parse(&name)
            .map(Some)
            .map_err(|error| host(&name, error))
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
        if let Home::Unmounted { scratch } = &self.home {
            return self.create_unmounted(directory, refquota, scratch);
        }
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
        // At home in a store that is not a dataset the clone is unmounted; otherwise it is
        // mounted at its image path.
        let placement = match self.home {
            Home::InStore => vec![format!("mountpoint={}", path_text(destination)?)],
            Home::Unmounted { .. } => vec![
                String::from("canmount=noauto"),
                String::from("mountpoint=none"),
            ],
        };
        let quota = (quota != "0").then(|| format!("refquota={quota}"));
        let mut arguments = vec!["clone", snapshot.as_str(), name.as_str()];
        arguments.extend(placement.iter().map(String::as_str));
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

    /// Destroy the containers of the project whose main payload is `main`, once it is being
    /// unbound and every payload of it is reclaimed: `ws`, `build` and the `.`-named containers in
    /// them (`.trash`, `.deleting`), then the project's own, then its owner's when no other
    /// project is left in it. Deepest first, each with a plain `destroy`, so ZFS itself refuses one
    /// that still holds anything; a dataset or snapshot below the project that is no container
    /// refuses before anything goes, named.
    pub(super) fn destroy_project_containers(&self, main: &Path) -> Result<(), ApfsStorageError> {
        let (containers, _) = self.placed(main)?;
        let [.., owner, project] = containers.as_slice() else {
            return Err(host("no project container above", main.display()));
        };
        if !self.exists(project) {
            return Ok(());
        }
        let listed = self.zfs(&["list", "-H", "-r", "-t", "all", "-o", "name", project])?;
        let mut names = listed.lines().collect::<Vec<_>>();
        let payloads = names
            .iter()
            .filter(|name| !is_project_container(project, name))
            .copied()
            .collect::<Vec<_>>();
        if !payloads.is_empty() {
            return Err(host(
                format_args!("{project} still holds what is no container"),
                payloads.join(", "),
            ));
        }
        names.sort_by_key(|name| std::cmp::Reverse(name.matches('/').count()));
        for name in names {
            self.helper(&["destroy", name])?;
        }
        let siblings = self.zfs(&["list", "-H", "-d", "1", "-o", "name", owner])?;
        if siblings.lines().all(|name| name == owner) {
            self.helper(&["destroy", owner])?;
        }
        Ok(())
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
        if self.property(dataset.as_str(), "mounted")? == "yes" {
            self.helper(&["unmount", dataset.as_str(), "--force"])?;
        }
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

/// The mount protocol of a store that is not a dataset ([`Home::Unmounted`]). The record is the
/// directory store's: while a payload is mounted its image path is a symlink naming its bind
/// place, and while it is home its image path is an empty directory and its dataset, the one that
/// path names, is unmounted with `mountpoint=none` and `canmount=noauto`. A move writes the new
/// record beside the image path (its move sibling) first, then moves the dataset, then lets the
/// record take the image path's place; the kernel's mount table decides a move a crash
/// interrupted.
impl ZfsPayloads {
    /// Make the empty home payload at `image`, capped at `refquota` bytes: created mounted once in
    /// `scratch`, where the helper hands its root to this user, then sent home.
    fn create_unmounted(
        &self,
        image: &Path,
        refquota: Option<u64>,
        scratch: &Path,
    ) -> Result<(), ApfsStorageError> {
        let name = self.named(image)?;
        fs::create_dir(image).map_err(|error| io_error("create payload", image, error))?;
        let leaf = name.rsplit('/').next().unwrap_or_default();
        let handover = scratch.join(format!("{leaf}-{}", uuid::Uuid::new_v4().simple()));
        let made = fs::create_dir_all(&handover)
            .map_err(|error| io_error("create a new dataset's handover place", &handover, error))
            .and_then(|()| {
                let mountpoint = format!("mountpoint={}", path_text(&handover)?);
                let quota = refquota.map(|bytes| format!("refquota={bytes}"));
                let mut arguments = vec!["create", name.as_str(), mountpoint.as_str()];
                arguments.extend(quota.as_deref());
                self.helper(&arguments)
            })
            .and_then(|()| self.send_home(&name, false));
        let cleared = fs::remove_dir(&handover);
        if let Err(primary) = made {
            if self.exists(&name) {
                let _ = self.helper(&["destroy", &name]);
            }
            let _ = fs::remove_dir(image);
            return Err(primary);
        }
        cleared.map_err(|error| io_error("clear a new dataset's handover place", &handover, error))
    }

    /// Unmount `dataset`, out from under whoever holds it when `force`, and keep it unmountable
    /// where it is: home.
    fn send_home(&self, dataset: &str, force: bool) -> Result<(), ApfsStorageError> {
        if self.property(dataset, "mounted")? == "yes" {
            let mut arguments = vec!["unmount", dataset];
            arguments.extend(force.then_some("--force"));
            self.helper(&arguments)?;
        }
        self.helper(&["set", dataset, "canmount=noauto", "mountpoint=none"])
    }

    /// Mount `dataset` at `mount_point`, made when absent.
    fn mount_at(&self, dataset: &str, mount_point: &Path) -> Result<(), ApfsStorageError> {
        match fs::create_dir(mount_point) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error("create mountpoint", mount_point, error)),
        }
        let mountpoint = format!("mountpoint={}", path_text(mount_point)?);
        self.helper(&["set", dataset, "canmount=on", &mountpoint])?;
        // Setting the property mounts a dataset that may mount; one already mounted remounts.
        if !self.is_mounted(mount_point)? {
            self.helper(&["mount", dataset])?;
        }
        Ok(())
    }

    fn settle_unmounted(&self, image: &Path) -> Result<Payload, ApfsStorageError> {
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
                "payload is neither a home record nor a mount record",
                image.display(),
            ));
        }
        let (_, name) = self.placed(image)?;
        // A record waiting beside the image path names where a move in flight was taking the
        // payload: it is there when the dataset is mounted there.
        if mount_point_state(&sibling)? == MountPointState::Link {
            let mount_point = fs::read_link(&sibling)
                .map_err(|error| io_error("read mount record", &sibling, error))?;
            if self.is_mounted(&mount_point)? {
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
        if !self.exists(&name) {
            return Err(host(
                "no dataset is named by the payload at",
                image.display(),
            ));
        }
        // Anything else is home: the dataset goes, or stays, unmounted and unmountable, and the
        // image path is its home record again.
        self.send_home(&name, false)?;
        if metadata.is_none() {
            fs::create_dir(image)
                .map_err(|error| io_error("restore a payload's home", image, error))?;
        }
        if mount_point_state(&sibling)? == MountPointState::Link {
            clear_move_sibling(&sibling)?;
        }
        sync_parent_path(image)?;
        Ok(Payload::Home)
    }

    fn bind_unmounted(&self, image: &Path, mount_point: &Path) -> Result<(), ApfsStorageError> {
        let dataset = self
            .at_home(image)?
            .ok_or_else(|| host("no payload is home at", image.display()))?;
        let sibling = move_sibling(image);
        clear_move_sibling(&sibling)?;
        std::os::unix::fs::symlink(mount_point, &sibling)
            .map_err(|error| io_error("create mount record", &sibling, error))?;
        if let Err(primary) = self.mount_at(dataset.as_str(), mount_point) {
            let back = self.send_home(dataset.as_str(), false);
            return combine_cleanup(
                "mount payload",
                primary,
                back.and_then(|()| {
                    fs::remove_file(&sibling)
                        .map_err(|error| io_error("remove unused mount record", &sibling, error))
                }),
            );
        }
        fs::remove_dir(image)
            .map_err(|error| io_error("clear a mounted payload's home", image, error))?;
        fs::rename(&sibling, image).map_err(|error| io_error("record mount", image, error))?;
        sync_parent_path(image)
    }

    fn move_home_unmounted(
        &self,
        image: &Path,
        mount_point: &Path,
        force: bool,
    ) -> Result<(), ApfsStorageError> {
        let dataset = self.payload_at(mount_point)?;
        let sibling = move_sibling(image);
        clear_move_sibling(&sibling)?;
        fs::rename(image, &sibling)
            .map_err(|error| io_error("set a mount record aside", image, error))?;
        if let Err(primary) = self.send_home(dataset.as_str(), force) {
            let rollback = fs::rename(&sibling, image)
                .map_err(|error| io_error("restore mount record", image, error));
            return combine_cleanup("unmount payload", primary, rollback);
        }
        fs::create_dir(image)
            .map_err(|error| io_error("restore a payload's home", image, error))?;
        fs::remove_file(&sibling)
            .map_err(|error| io_error("remove released mount record", &sibling, error))?;
        sync_parent_path(image)
    }

    fn rename_unmounted(&self, from: &Path, to: &Path) -> Result<(), ApfsStorageError> {
        let dataset = self
            .at_home(from)?
            .ok_or_else(|| host("no payload is home at", from.display()))?;
        let renamed = self.named(to)?;
        self.rename_dataset(dataset.as_str(), &renamed)?;
        fs::rename(from, to).map_err(|error| io_error("move a payload's home", from, error))?;
        sync_parent_path(to)?;
        sync_parent_path(from)
    }

    /// Give `dataset` the name `renamed`, and its fork's origin snapshot the fork's new name,
    /// which leaves the old name free for the next fork of that name.
    fn rename_dataset(&self, dataset: &str, renamed: &str) -> Result<(), ApfsStorageError> {
        if dataset == renamed {
            return Ok(());
        }
        self.helper(&["rename", dataset, renamed])?;
        let leaf = |name: &str| name.rsplit('/').next().unwrap_or_default().to_owned();
        let origin = self.property(renamed, "origin")?;
        if let Some((source, snapshot)) = origin.split_once('@')
            && snapshot == format!("cowshed:fork-{}", leaf(dataset))
        {
            let followed = format!("{source}@cowshed:fork-{}", leaf(renamed));
            self.helper(&["rename", &origin, &followed])?;
        }
        Ok(())
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
        if matches!(self.home, Home::Unmounted { .. }) {
            return self.settle_unmounted(image);
        }
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
        if matches!(self.home, Home::Unmounted { .. }) {
            return self.bind_unmounted(image, mount_point);
        }
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
        if matches!(self.home, Home::Unmounted { .. }) {
            return self.move_home_unmounted(image, mount_point, force);
        }
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
        if matches!(self.home, Home::Unmounted { .. }) {
            return self.rename_unmounted(from, to);
        }
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
mod laws;

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
        // `zfs get -H -o name,value,source cowshed:owner`: only an owner set on the root itself
        // makes it one; a dataset named `cowshed` that inherits its owner from a root above it is a
        // dataset of that root, never a second root.
        let owners = "tank/cowshed\t501\tlocal\ntank/x/cowshed\t502\tlocal\ntank/y/cowshed\t-\t-\n\
                      tank/cowshed/store\t501\tinherited from tank/cowshed\ntank/z\t501\tlocal\n\
                      tank/cowshed/projects/t/unowned/cowshed\t501\tinherited from tank/cowshed\n\
                      tank/r/cowshed\t501\treceived\n";
        let names = |uid| {
            serving_roots(owners, uid)
                .iter()
                .map(|root| root.dataset().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(501), ["tank/cowshed"]);
        assert!(names(500).is_empty());
    }

    /// A project no root serves is refused naming the user, with the command that makes a root;
    /// one several roots would serve is refused naming each.
    #[test]
    fn an_unserved_project_names_the_user_and_every_root() {
        let roots = ["tank/cowshed", "tank/b/cowshed"].map(|name| Root::parse(name).unwrap());
        assert_eq!(
            ApfsStorageError::from(Unserved::NoRoot { uid: 501 }).to_string(),
            ApfsStorageError::Host(
                "no cowshed ZFS root names uid 501 as its owner: whoever administers a pool \
                 creates one: zfs create -o mountpoint=none -o cowshed:owner=501 <pool>/cowshed"
                    .to_owned()
            )
            .to_string()
        );
        assert_eq!(
            ApfsStorageError::from(Unserved::Several {
                uid: 501,
                roots: roots.to_vec(),
            })
            .to_string(),
            ApfsStorageError::Host(
                "several cowshed ZFS roots name uid 501 as their owner: tank/cowshed, tank/b/cowshed"
                    .to_owned()
            )
            .to_string()
        );
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

    /// A project's unbinding destroys its containers and nothing else: no payload, retired or
    /// not, and no snapshot.
    #[test]
    fn only_a_projects_containers_are_released_with_it() {
        let project = "tank/cowshed/projects/acme/widget";
        for container in [
            "",
            "/ws",
            "/build",
            "/ws/.trash",
            "/build/.trash",
            "/build/.deleting",
        ] {
            assert!(super::is_project_container(
                project,
                &format!("{project}{container}")
            ));
        }
        for payload in [
            "/main",
            "/ws/raven",
            "/build/0a1b",
            "/ws/.trash/raven-1",
            "/build/.trash/0a1b",
            "/main@cowshed:fork-raven",
            "/ws@x",
            "-other",
        ] {
            assert!(!super::is_project_container(
                project,
                &format!("{project}{payload}")
            ));
        }
        assert!(!super::is_project_container(
            project,
            "tank/cowshed/projects/acme"
        ));
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
