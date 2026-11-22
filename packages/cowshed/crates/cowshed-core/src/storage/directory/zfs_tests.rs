//! Dataset payloads against real ZFS (specs/cowshed/08_testing.md, "Real ZFS"): the store's
//! mount protocol over a store that is not a dataset, as macOS's APFS store is, a fork's clone
//! and what a reclaim destroys, through the installed root helper. An explicit host check, never
//! part of an ordinary test run: it works below the cowshed root that names this user,
//! `${COWSHED_ZFS_TEST_PARENT}/cowshed`, in a project of its own, and runs every mutation through
//! the helper setup installed, with `sudo -n`. Nothing asks for a password; a host where that is
//! not proven skips, saying why.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_apfs::DetachIntent;
use cowshed_os::test_backstop::CommandBackstop as _;

use super::zfs::ZfsPayloads;
use super::{Payload, Payloads, bind, observed_mount, settle, unbind};

fn zfs(arguments: &[&str]) -> Option<String> {
    let output = Command::new("zfs")
        .args(arguments)
        .env("PATH", cowshed_helper::host::TRUSTED_PATH)
        .output_bounded()
        .expect("zfs runs");
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// A project of this run's own, `cowshed-itest-<id>/widget`, in a plain store directory, kept as
/// datasets below the cowshed root that names this user. Its datasets are destroyed, leaf first
/// and through the helper, on every exit path.
struct Fixture {
    directory: PathBuf,
    store: PathBuf,
    owner: String,
    payloads: Payloads,
}

impl Fixture {
    /// The fixture, or `None` with the reason this host cannot run the check printed: no parent
    /// named, no `zfs`, no root there that names this user locally, or no installed helper that
    /// acts as root for it without a password.
    fn new() -> Option<Self> {
        let skip = |reason: String| {
            cowshed_os::eprintln!("skipped: {reason}");
            None
        };
        let Ok(parent) = std::env::var("COWSHED_ZFS_TEST_PARENT") else {
            return skip(String::from(
                "COWSHED_ZFS_TEST_PARENT names no pool or dataset whose cowshed root to use",
            ));
        };
        let root = format!("{parent}/cowshed");
        // SAFETY: reads this process's real uid.
        let uid = unsafe { libc::getuid() };
        let owner = zfs(&[
            "get",
            "-H",
            "-o",
            "value,source",
            crate::storage::bootstrap::ZFS_OWNER_PROPERTY,
            &root,
        ]);
        if owner.as_deref() != Some(&format!("{uid}\tlocal")) {
            return skip(format!(
                "{root} is not a cowshed root whose owner property names uid {uid} locally (read: {owner:?})"
            ));
        }
        let id = uuid::Uuid::new_v4().simple().to_string();
        let owner = format!("cowshed-itest-{id}");
        // The per-user fixture directory beside the mount root (08_testing.md, "Fixtures"), which
        // keeps ownership: a job's temporary directory lies in the store, whose volume ignores it,
        // and the helper refuses a mountpoint whose owner it cannot read as the caller's.
        let home = PathBuf::from(std::env::var_os("HOME").expect("HOME"));
        let parent_name = parent.clone();
        let parent = crate::storage::host_config::canonical_mount_root(&home)
            .parent()
            .expect("the mount root has a parent")
            .join("t");
        fs::create_dir_all(&parent).expect("the per-user fixture directory");
        let directory = parent.join(format!("cowshed-zfs-{id}"));
        let store = directory.join("store");
        fs::create_dir_all(store.join(&owner).join("widget")).expect("the scratch store is new");
        let main = store.join(&owner).join("widget/main.dmg");
        // The parent is declared, as a project's `.cowshed.toml` declares it: a host may hold
        // several roots that name this user.
        let datasets = match ZfsPayloads::of_project(
            &store,
            &main,
            true,
            &directory.join("scratch"),
            Some(&parent_name),
        ) {
            Ok(Some(datasets)) if datasets.root().dataset().as_str() == root => datasets,
            other => {
                let _ = fs::remove_dir_all(&directory);
                return skip(format!(
                    "the root this user's adoption selects is not {root}: {other:?}"
                ));
            }
        };
        if let Err(error) = datasets.prove_helper() {
            let _ = fs::remove_dir_all(&directory);
            return skip(format!(
                "the installed root helper does not act as root here: {error}"
            ));
        }
        Some(Self {
            directory,
            store,
            owner,
            payloads: Payloads::Datasets(datasets),
        })
    }

    fn datasets(&self) -> &ZfsPayloads {
        match &self.payloads {
            Payloads::Datasets(datasets) => datasets,
            Payloads::Directories => unreachable!("the fixture's project keeps datasets"),
        }
    }

    fn image(&self, relative: &str) -> PathBuf {
        let image = self.store.join(&self.owner).join("widget").join(relative);
        fs::create_dir_all(image.parent().expect("a parent")).expect("the project tree");
        image
    }

    /// A bind place of this run, made by this user.
    fn place(&self, relative: &str) -> PathBuf {
        let place = self.directory.join(relative);
        fs::create_dir_all(&place).expect("the bind place");
        place
    }

    fn container(&self) -> String {
        format!(
            "{}/projects/{}/widget",
            self.datasets().root().dataset(),
            self.owner
        )
    }

    /// Every dataset and snapshot of the project, named relative to its container.
    fn listed(&self) -> Vec<String> {
        let container = self.container();
        zfs(&["list", "-H", "-r", "-t", "all", "-o", "name", &container])
            .map(|names| {
                names
                    .lines()
                    .filter_map(|name| name.strip_prefix(&container)?.strip_prefix('/'))
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `mounted`, `mountpoint` and `canmount` of the payload `relative` names.
    fn mounting(&self, relative: &str) -> String {
        zfs(&[
            "get",
            "-H",
            "-o",
            "value",
            "mounted,mountpoint,canmount",
            &format!("{}/{relative}", self.container()),
        ])
        .expect("its properties are readable")
        .replace('\n', " ")
    }

    /// The dataset mounted at `directory`.
    fn dataset(&self, directory: &Path) -> String {
        let opened = fs::File::open(directory).expect("the directory opens");
        cowshed_helper::host::mounted_dataset(&opened)
            .expect("its mount is readable")
            .expect("a dataset is mounted there")
    }

    fn origin(&self, directory: &Path) -> String {
        zfs(&[
            "get",
            "-H",
            "-o",
            "value",
            "origin",
            &self.dataset(directory),
        ])
        .expect("its origin is readable")
    }

    /// One verb of the installed helper on this root, as the product runs it.
    fn helper(&self, arguments: &[&str]) -> std::process::Output {
        let datasets = self.datasets();
        Command::new("sudo")
            .arg("-n")
            .arg(datasets.helper_path())
            .arg(datasets.root().dataset().as_str())
            .args(arguments)
            .output_bounded()
            .expect("sudo runs")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Leaf first through the helper, which never recurses: a clone goes before its origin,
        // so every pass destroys what nothing depends on until nothing is left.
        let project = format!(
            "{}/projects/{}",
            self.datasets().root().dataset(),
            self.owner
        );
        let mut left: Vec<String>;
        loop {
            left = zfs(&["list", "-H", "-r", "-t", "all", "-o", "name", &project])
                .map(|names| names.lines().map(str::to_owned).collect())
                .unwrap_or_default();
            if left.is_empty() {
                break;
            }
            let before = left.len();
            for name in left.iter().rev() {
                let _ = self.helper(&["destroy", name]);
            }
            let after = zfs(&["list", "-H", "-r", "-t", "all", "-o", "name", &project])
                .map(|names| names.lines().count())
                .unwrap_or_default();
            if after == before {
                break;
            }
        }
        let removed = fs::remove_dir_all(&self.directory);
        if !std::thread::panicking() {
            assert_eq!(
                left,
                Vec::<String>::new(),
                "the project's datasets are destroyed"
            );
            assert!(
                removed.is_ok(),
                "the scratch directory is removed: {removed:?}"
            );
        }
    }
}

//#region smoo/cowshed!n/macos-dataset-payload-home
#[test]
#[ignore = "explicit host check: real ZFS through the installed helper (specs/cowshed/08_testing.md, Real ZFS)"]
fn dataset_payloads_mount_only_at_their_bind_place_fork_and_reclaim_as_the_store_protocol_says() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    let (payloads, datasets) = (&fixture.payloads, fixture.datasets());
    let census = crate::census::this_test();

    // A new payload is home: unmounted, in no place the store's filesystem decides the owner of.
    let main = fixture.image("main.dmg");
    datasets.create(&main, Some(1 << 30)).unwrap();
    assert_eq!(settle(payloads, &main).unwrap(), Payload::Home);
    assert_eq!(fixture.mounting("main"), "no none noauto");
    assert_eq!(fixture.listed(), ["main"]);

    // Mounting puts the dataset at its bind place, the caller's, and leaves the store's record.
    let checkout = fixture.directory.join("checkout");
    bind(payloads, &main, &checkout).unwrap();
    assert_eq!(fs::read_link(&main).unwrap(), checkout);
    assert_eq!(observed_mount(&main).unwrap(), Some(checkout.clone()));
    assert_eq!(
        settle(payloads, &main).unwrap(),
        Payload::Mounted(checkout.clone())
    );
    fs::write(checkout.join("tracked"), "main").expect("the new dataset's root is this user's");
    bind(payloads, &main, &checkout).unwrap();
    assert!(bind(payloads, &main, &fixture.directory.join("elsewhere")).is_err());

    // A fork is a clone of a snapshot of its mounted source: it starts with the source's files,
    // keeps its cap, is home until bound, and neither sees the other's later writes.
    let raven = fixture.image("sessions/raven.dmg");
    datasets.clone_to(&checkout, &raven).unwrap();
    assert_eq!(settle(payloads, &raven).unwrap(), Payload::Home);
    assert_eq!(fixture.mounting("ws/raven"), "no none noauto");
    let workspace = fixture.place("mnt/raven");
    bind(payloads, &raven, &workspace).unwrap();
    assert!(fixture.origin(&workspace).contains("@cowshed:fork-raven"));
    fs::write(workspace.join("tracked"), "raven").unwrap();
    assert_eq!(
        fs::read_to_string(checkout.join("tracked")).unwrap(),
        "main"
    );
    assert_eq!(
        fixture.listed(),
        ["main", "main@cowshed:fork-raven", "ws", "ws/raven"]
    );

    // A workspace dataset with a live fork is never destroyed or promoted over: it is kept.
    assert_eq!(datasets.destroy(&checkout).unwrap(), None);
    assert_eq!(
        fs::read_to_string(checkout.join("tracked")).unwrap(),
        "main"
    );

    // A mount lost without an unmount of the store's (a reboot, an import that did not mount)
    // is mounted again at its bind place.
    let clone = fixture.dataset(&workspace);
    assert!(fixture.helper(&["unmount", &clone]).status.success());
    assert!(!workspace.join("tracked").exists());
    assert_eq!(
        settle(payloads, &raven).unwrap(),
        Payload::Mounted(workspace.clone())
    );
    assert_eq!(
        fs::read_to_string(workspace.join("tracked")).unwrap(),
        "raven"
    );

    // Unmounting brings the dataset home, unmounted again, and leaves the bind place empty; home,
    // it moves like a directory payload (a retirement's rename).
    unbind(payloads, &census, &raven, &workspace, DetachIntent::Release).unwrap();
    assert_eq!(settle(payloads, &raven).unwrap(), Payload::Home);
    assert_eq!(fixture.mounting("ws/raven"), "no none noauto");
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    let retired = fixture.image("sessions/.trash/raven-1.dmg");
    datasets.rename(&raven, &retired).unwrap();
    assert!(!raven.exists());
    // The dataset and its origin snapshot carry the retired name, so `raven` is free again.
    assert_eq!(
        fixture.listed(),
        [
            "main",
            "main@cowshed:fork-raven-1",
            "ws",
            "ws/.trash",
            "ws/.trash/raven-1"
        ]
    );
    assert_eq!(fixture.mounting("ws/.trash/raven-1"), "no none noauto");
    let kept = fixture.place("mnt/kept");
    bind(payloads, &retired, &kept).unwrap();
    assert_eq!(fs::read_to_string(kept.join("tracked")).unwrap(), "raven");
    unbind(payloads, &census, &retired, &kept, DetachIntent::Release).unwrap();

    // Reclaiming destroys the clone and then its origin snapshot, and only those.
    assert!(datasets.destroy(&retired).unwrap().is_some());
    assert!(!retired.exists());
    assert_eq!(fixture.listed(), ["main", "ws", "ws/.trash"]);
    assert_eq!(
        fs::read_to_string(checkout.join("tracked")).unwrap(),
        "main"
    );

    // A build dataset that is another's origin hands its lineage over before it goes: the fork's
    // volume survives its seed.
    let seed = fixture.image("build/1.dmg");
    datasets.create(&seed, None).unwrap();
    let seeded = fixture.place("build/1");
    bind(payloads, &seed, &seeded).unwrap();
    fs::write(seeded.join("cache"), "warm").unwrap();
    let live = fixture.image("build/2.dmg");
    datasets.clone_to(&seeded, &live).unwrap();
    assert!(datasets.destroy(&seeded).unwrap().is_some());
    let warm = fixture.place("build/2");
    bind(payloads, &live, &warm).unwrap();
    assert_eq!(fs::read_to_string(warm.join("cache")).unwrap(), "warm");
    assert_eq!(fixture.origin(&warm), "-");
    assert_eq!(
        fixture.listed(),
        ["build", "build/2", "main", "ws", "ws/.trash"]
    );
}
//#endregion smoo/cowshed!n/macos-dataset-payload-home

/// A payload's cap is its dataset's `refquota`: read from the dataset whether it is home or
/// mounted, raised in place while the payload is mounted with a file open, and absent from a
/// dataset made without one. Mounted at its own root, the dataset is the volume a job's usage
/// samples read.
#[test]
#[ignore = "explicit host check: real ZFS through the installed helper (specs/cowshed/08_testing.md, Real ZFS)"]
fn a_dataset_payloads_capacity_is_its_refquota_raised_in_place() {
    use crate::runtime::volume_usage::{HostVolume, HostVolumeStat, VolumeStat as _};

    let Some(fixture) = Fixture::new() else {
        return;
    };
    let (payloads, datasets) = (&fixture.payloads, fixture.datasets());
    const GIBIBYTE: u64 = 1 << 30;

    let main = fixture.image("main.dmg");
    datasets.create(&main, Some(GIBIBYTE)).unwrap();
    assert_eq!(datasets.capacity(&main).unwrap(), Some(GIBIBYTE));

    let checkout = fixture.directory.join("checkout");
    bind(payloads, &main, &checkout).unwrap();
    let held = fs::File::create(checkout.join("held")).unwrap();
    datasets.set_capacity(&checkout, 2 * GIBIBYTE).unwrap();
    assert_eq!(datasets.capacity(&checkout).unwrap(), Some(2 * GIBIBYTE));
    drop(held);

    let (volume, _) =
        HostVolume::capture(&checkout).expect("a dataset mounted at its root is a volume");
    HostVolumeStat
        .used_bytes(&volume)
        .expect("the same dataset is still mounted there");

    let uncapped = fixture.image("build/1.dmg");
    datasets.create(&uncapped, None).unwrap();
    assert_eq!(datasets.capacity(&uncapped).unwrap(), None);
}

/// Setup's root decision over what [`super::root_evidence`] reads on a real pool
/// (01b_zfs.md, "One pool, one root"): the root naming this user, as setup made it, is taken on
/// macOS with its four com.apple properties and elsewhere by its owner; a root that does not exist
/// below a parent that does reads as absent, so the decision would create it, and one whose parent
/// does not exist either reads as an orphan, which setup refuses before any dialog; neither is an
/// error. Only reads: nothing changes and nothing asks for privilege.
#[test]
#[ignore = "explicit host check: real ZFS through the installed helper (specs/cowshed/08_testing.md, Real ZFS)"]
fn setup_reads_and_takes_the_root_that_names_the_user() {
    use crate::storage::bootstrap::zfs_root::{RootEvidence, RootStep, root_steps};
    let Ok(parent) = std::env::var("COWSHED_ZFS_TEST_PARENT") else {
        cowshed_os::eprintln!(
            "skipped: COWSHED_ZFS_TEST_PARENT names no pool or dataset whose cowshed root to use"
        );
        return;
    };
    if !super::zfs_installed() {
        cowshed_os::eprintln!("skipped: this host has no zfs");
        return;
    }
    // SAFETY: reads this process's real uid.
    let uid = unsafe { libc::getuid() };
    let apple = cfg!(target_os = "macos");
    let root = cowshed_helper::name::Root::parse(&format!("{parent}/cowshed")).unwrap();
    let steps = root_steps(true, Some(&root), &[], uid, apple, |root| {
        super::root_evidence(root, apple)
    })
    .unwrap();
    assert_eq!(
        steps,
        vec![RootStep::Take {
            root: root.dataset().to_string()
        }]
    );

    // `<root>/projects` exists below the root setup made; nothing named `cowshed` is below it.
    let absent =
        cowshed_helper::name::Root::parse(&format!("{parent}/cowshed/projects/cowshed")).unwrap();
    assert_eq!(
        super::root_evidence(&absent, apple).unwrap(),
        RootEvidence::Absent
    );
    let orphan = cowshed_helper::name::Root::parse(&format!(
        "{parent}/cowshed-itest-absent-{}/cowshed",
        std::process::id()
    ))
    .unwrap();
    assert_eq!(
        super::root_evidence(&orphan, apple).unwrap(),
        RootEvidence::Orphan
    );
}
