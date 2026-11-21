//! Dataset payloads against real ZFS (specs/cowshed/08_testing.md, "Real ZFS"): the store's
//! mount protocol, a fork's clone and what a reclaim destroys, through the real helper. A host
//! proof: it needs `sudo -n`, `cowshed-helper` on `PATH`, and `COWSHED_ZFS_TEST_PARENT`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_apfs::DetachIntent;
use cowshed_os::test_backstop::CommandBackstop as _;

use super::zfs::ZfsPayloads;
use super::{Payload, Payloads, bind, observed_mount, settle, unbind};

fn sudo(arguments: &[&str]) -> std::process::Output {
    Command::new("sudo")
        .arg("-n")
        .args(arguments)
        .output_bounded()
        .expect("sudo runs")
}

fn zfs(arguments: &[&str]) -> Option<String> {
    let output = Command::new("zfs")
        .args(arguments)
        .output_bounded()
        .expect("zfs runs");
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// A scratch cowshed root that serves this user, its `store` dataset mounted at a scratch store
/// directory, destroyed on every exit path.
struct Fixture {
    tree: String,
    directory: PathBuf,
    payloads: Payloads,
}

impl Fixture {
    fn new() -> Self {
        let parent = std::env::var("COWSHED_ZFS_TEST_PARENT")
            .expect("COWSHED_ZFS_TEST_PARENT names the dataset this host proof may create below");
        let id = uuid::Uuid::new_v4().simple().to_string();
        let tree = format!("{parent}/cowshed-itest-{id}");
        let directory = fs::canonicalize(std::env::temp_dir())
            .expect("the temporary directory resolves")
            .join(format!("cowshed-zfs-{id}"));
        fs::create_dir(&directory).expect("the scratch directory is new");
        let store = directory.join("store");
        let uid =
            std::os::unix::fs::MetadataExt::uid(&fs::metadata(&directory).expect("it exists"));
        let root = format!("{tree}/cowshed");
        for arguments in [
            vec!["zfs", "create", "-o", "mountpoint=none", &tree],
            vec![
                "zfs",
                "create",
                "-o",
                &format!("cowshed:owner={uid}"),
                &root,
            ],
        ] {
            let made = sudo(&arguments);
            assert!(made.status.success(), "{arguments:?}: {made:?}");
        }
        let helper = std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
            .map(|directory| directory.join("cowshed-helper"))
            .find(|candidate| candidate.is_file())
            .expect("cowshed-helper is on PATH (cargo build -p cowshed-helper)");
        let mounted = sudo(&[
            helper.to_str().expect("UTF-8"),
            &root,
            "create",
            &format!("{root}/store"),
            &format!("mountpoint={}", store.display()),
        ]);
        assert!(mounted.status.success(), "{mounted:?}");
        let payloads = ZfsPayloads::of_store(&store)
            .expect("the store's mount is readable")
            .expect("a store that is a cowshed root's store dataset keeps dataset payloads");
        Self {
            tree,
            directory,
            payloads: Payloads::Datasets(payloads),
        }
    }

    fn datasets(&self) -> &ZfsPayloads {
        match &self.payloads {
            Payloads::Datasets(datasets) => datasets,
            Payloads::Directories => unreachable!("the fixture's store is a dataset"),
        }
    }

    fn image(&self, relative: &str) -> PathBuf {
        let image = self.directory.join("store/acme/widget").join(relative);
        fs::create_dir_all(image.parent().expect("a parent")).expect("the project tree");
        image
    }

    /// The datasets and snapshots below the project's container `kind`.
    fn listed(&self, kind: &str) -> Vec<String> {
        let container = format!("{}/cowshed/projects/acme/widget/{kind}", self.tree);
        zfs(&["list", "-H", "-r", "-t", "all", "-o", "name", &container])
            .map(|names| {
                names
                    .lines()
                    .filter(|name| *name != container)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
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
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Home payloads are mounted inside the store's mount, so they go before the store does.
        let _ = sudo(&[
            "zfs",
            "destroy",
            "-R",
            "-f",
            &format!("{}/cowshed/projects", self.tree),
        ]);
        let destroyed = sudo(&["zfs", "destroy", "-R", "-f", &self.tree]);
        let removed = sudo(&["rm", "-rf", "--", self.directory.to_str().expect("UTF-8")]);
        if !std::thread::panicking() {
            assert!(destroyed.status.success(), "{destroyed:?}");
            assert!(removed.status.success(), "{removed:?}");
        }
    }
}

#[test]
#[ignore = "host proof: needs sudo, cowshed-helper on PATH and COWSHED_ZFS_TEST_PARENT (nx run cowshed:zfs-test)"]
fn dataset_payloads_mount_fork_and_reclaim_as_the_store_protocol_says() {
    let fixture = Fixture::new();
    let (payloads, datasets) = (&fixture.payloads, fixture.datasets());
    let census = crate::census::this_test();

    // A new payload is home: its dataset is mounted at its image path, and it is this user's.
    let main = fixture.image("main.dmg");
    datasets.create(&main, Some(1 << 30)).unwrap();
    assert_eq!(settle(payloads, &main).unwrap(), Payload::Home);
    fs::write(main.join("tracked"), "main").unwrap();
    assert_eq!(fixture.listed("ws").len(), 1);

    // Mounting moves the dataset onto the mountpoint and leaves the directory store's record.
    let checkout = fixture.directory.join("checkout");
    bind(payloads, &main, &checkout).unwrap();
    assert_eq!(fs::read_link(&main).unwrap(), checkout);
    assert_eq!(observed_mount(&main).unwrap(), Some(checkout.clone()));
    assert_eq!(
        settle(payloads, &main).unwrap(),
        Payload::Mounted(checkout.clone())
    );
    assert_eq!(
        fs::read_to_string(checkout.join("tracked")).unwrap(),
        "main"
    );
    bind(payloads, &main, &checkout).unwrap();
    assert!(bind(payloads, &main, &fixture.directory.join("elsewhere")).is_err());

    // A fork is a clone of a snapshot of its mounted source: it starts with the source's files,
    // keeps its cap, and neither sees the other's later writes.
    let raven = fixture.image("sessions/raven.dmg");
    datasets.clone_to(&checkout, &raven).unwrap();
    assert_eq!(settle(payloads, &raven).unwrap(), Payload::Home);
    assert!(fixture.origin(&raven).contains("@cowshed:fork-"));
    let workspace = fixture.directory.join("mnt/raven");
    fs::create_dir_all(workspace.parent().unwrap()).unwrap();
    bind(payloads, &raven, &workspace).unwrap();
    fs::write(workspace.join("tracked"), "raven").unwrap();
    assert_eq!(
        fs::read_to_string(checkout.join("tracked")).unwrap(),
        "main"
    );
    assert_eq!(fixture.listed("ws").len(), 3, "{:?}", fixture.listed("ws"));

    // A workspace dataset with a live fork is never destroyed or promoted over: it is kept.
    assert_eq!(datasets.destroy(&checkout).unwrap(), None);
    assert_eq!(
        fs::read_to_string(checkout.join("tracked")).unwrap(),
        "main"
    );

    // A mount lost without an unmount of the store's (a reboot, an import that did not mount)
    // is mounted again from the dataset's own mountpoint.
    let clone = fixture.dataset(&workspace);
    assert!(sudo(&["zfs", "unmount", &clone]).status.success());
    assert!(!workspace.join("tracked").exists());
    assert_eq!(
        settle(payloads, &raven).unwrap(),
        Payload::Mounted(workspace.clone())
    );
    assert_eq!(
        fs::read_to_string(workspace.join("tracked")).unwrap(),
        "raven"
    );

    // Unmounting brings the dataset home and leaves an empty mountpoint; home, it moves like a
    // directory payload (a retirement's rename).
    unbind(payloads, &census, &raven, &workspace, DetachIntent::Release).unwrap();
    assert_eq!(settle(payloads, &raven).unwrap(), Payload::Home);
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    assert_eq!(fs::read_to_string(raven.join("tracked")).unwrap(), "raven");
    let retired = fixture.image("sessions/.trash/raven-1.dmg");
    datasets.rename(&raven, &retired).unwrap();
    assert!(!raven.exists());
    assert_eq!(
        fs::read_to_string(retired.join("tracked")).unwrap(),
        "raven"
    );

    // Reclaiming destroys the clone and then its origin snapshot, and only those.
    assert!(datasets.destroy(&retired).unwrap().is_some());
    assert!(!retired.exists());
    assert_eq!(fixture.listed("ws").len(), 1, "{:?}", fixture.listed("ws"));
    assert_eq!(
        fs::read_to_string(checkout.join("tracked")).unwrap(),
        "main"
    );

    // A build dataset that is another's origin hands its lineage over before it goes: the fork's
    // volume survives its seed.
    let seed = fixture.image("build/1.dmg");
    datasets.create(&seed, None).unwrap();
    fs::write(seed.join("cache"), "warm").unwrap();
    let live = fixture.image("build/2.dmg");
    datasets.clone_to(&seed, &live).unwrap();
    assert!(datasets.destroy(&seed).unwrap().is_some());
    assert_eq!(fs::read_to_string(live.join("cache")).unwrap(), "warm");
    assert_eq!(fixture.origin(&live), "-");
    assert_eq!(
        fixture.listed("build").len(),
        1,
        "{:?}",
        fixture.listed("build")
    );
}
