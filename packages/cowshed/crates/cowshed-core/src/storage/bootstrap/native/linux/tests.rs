//! The Linux adapter's decisions over typed evidence: no host is read and nothing is run.

use std::collections::BTreeMap;

use super::*;

const UID: u32 = 1000;
const OWNER: DirectoryOwner = DirectoryOwner { uid: UID, gid: 100 };
const STORE: &str = "/private/cowshed/store";
const BUILD_CACHE: &str = "/private/cowshed/build-cache";
const HOME: &str = "/home/dev";

fn mount(id: u64, mountpoint: &str, fs_type: &str, source: &str) -> Mount {
    Mount {
        id,
        root: "/".to_owned(),
        mountpoint: PathBuf::from(mountpoint),
        fs_type: fs_type.to_owned(),
        source: source.to_owned(),
    }
}

fn directory(uid: u32, mode: u32, empty: bool, marker: Option<Vec<u8>>) -> PathEntry {
    PathEntry::Directory(DirectoryEntry {
        uid,
        mode,
        empty,
        marker,
    })
}

fn marker(role: VolumeRole, substrate: SubstrateKind) -> Vec<u8> {
    VolumeMarker::new(role, substrate).to_json().unwrap()
}

fn dataset(owner: Option<&str>, role: Option<VolumeRole>) -> Dataset {
    Dataset {
        guid: "4242".to_owned(),
        used: 98_304,
        owner: owner.map(str::to_owned),
        marker: role.map_or(ExistingMarkerEvidence::Missing, |role| {
            ExistingMarkerEvidence::Valid(VolumeMarker::new(role, SubstrateKind::Zfs))
        }),
    }
}

/// A host as the planners read it: a root filesystem, and whatever a case adds.
struct Host {
    mounts: Vec<Mount>,
    entries: BTreeMap<PathBuf, PathEntry>,
    datasets: BTreeMap<String, Dataset>,
    config_missing: bool,
    only_marker: bool,
}

impl Host {
    /// An ext4 root with a home in it and nothing of cowshed's.
    fn bare() -> Self {
        let mut host = Self {
            mounts: vec![mount(1, "/", "ext4", "/dev/vda1")],
            entries: BTreeMap::new(),
            datasets: BTreeMap::new(),
            config_missing: true,
            only_marker: true,
        };
        host.at("/", directory(0, 0o755, false, None));
        host.at("/home", directory(0, 0o755, false, None));
        host.at(HOME, directory(UID, 0o700, false, None));
        host
    }

    fn at(&mut self, path: &str, entry: PathEntry) -> &mut Self {
        self.entries.insert(PathBuf::from(path), entry);
        self
    }

    fn with(&mut self, name: &str, dataset: Dataset) -> &mut Self {
        self.datasets.insert(name.to_owned(), dataset);
        self
    }

    fn mounted(&mut self, id: u64, mountpoint: &str, fs_type: &str, source: &str) -> &mut Self {
        self.mounts.push(mount(id, mountpoint, fs_type, source));
        self
    }
}

impl EvidenceSource for Host {
    fn mounts(&mut self) -> Result<Vec<Mount>, NativeBootstrapError> {
        Ok(self.mounts.clone())
    }

    fn entry(&mut self, path: &Path) -> Result<PathEntry, NativeBootstrapError> {
        Ok(self
            .entries
            .get(path)
            .cloned()
            .unwrap_or(PathEntry::Missing))
    }

    fn dataset(&mut self, name: &str) -> Result<Option<Dataset>, NativeBootstrapError> {
        Ok(self.datasets.get(name).cloned())
    }

    fn host_config_missing(
        &mut self,
        _store: &Path,
        _home: &Path,
    ) -> Result<bool, NativeBootstrapError> {
        Ok(self.config_missing)
    }

    fn holds_only_marker(&mut self, _mountpoint: &Path) -> Result<bool, NativeBootstrapError> {
        Ok(self.only_marker)
    }

    fn owner(&mut self) -> DirectoryOwner {
        OWNER
    }
}

fn zfs_config(parent: &str) -> SubstrateConfig {
    parse_cowshed_config(&format!(
        "[substrate]\nkind = \"zfs\"\nparent = \"{parent}\"\n"
    ))
    .unwrap()
    .substrate()
    .unwrap()
    .clone()
}

fn command_lines(operations: &[HostOperation]) -> Vec<String> {
    operations
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::RunCommand(command) => Some(format!(
                "{} {}",
                command.program(),
                command.args().join(" ")
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn mountinfo_lines_parse_with_their_escapes_and_a_malformed_line_fails_the_read() {
    let table = "552 551 0:68 / / rw,noatime shared:705 - zfs tank/root rw,xattr\n\
                 623 552 0:41 /store /nix/store ro,noatime shared:798 master:6 - zfs tank/nix rw\n\
                 700 552 0:90 / /mnt/with\\040space rw - ext4 /dev/sdb1 rw\n";
    assert_eq!(
        parse_mountinfo(table).unwrap(),
        [
            mount(552, "/", "zfs", "tank/root"),
            Mount {
                root: "/store".to_owned(),
                ..mount(623, "/nix/store", "zfs", "tank/nix")
            },
            mount(700, "/mnt/with space", "ext4", "/dev/sdb1"),
        ]
    );
    for malformed in [
        "552 551 0:68 / / rw zfs tank rw",
        "x 1 0:1 / / rw - zfs",
        "",
    ] {
        assert!(
            parse_mountinfo(&format!("{malformed}\n")).is_err(),
            "{malformed:?}"
        );
    }
}

#[test]
fn a_path_lies_in_the_deepest_and_latest_mount_that_contains_it() {
    let mounts = [
        mount(1, "/", "ext4", "/dev/a"),
        mount(2, "/home", "ext4", "/dev/b"),
        mount(3, "/home", "btrfs", "/dev/c"),
        mount(4, "/homestead", "xfs", "/dev/d"),
    ];
    for (path, id) in [
        ("/home/dev/.cowshed/mnt", 3),
        ("/home", 3),
        ("/homestead/x", 4),
        ("/private/cowshed/store", 1),
    ] {
        assert_eq!(mount_of(&mounts, Path::new(path)).unwrap().id, id, "{path}");
    }
    assert_eq!(mount_at(&mounts, Path::new("/home")).unwrap().id, 3);
    assert!(mount_at(&mounts, Path::new("/home/dev")).is_none());
}

#[test]
fn one_directory_is_current_made_repaired_or_refused_by_what_holds_its_path() {
    let root_parent = directory(0, 0o755, false, None);
    let own_parent = directory(UID, 0o700, false, None);
    let root = Owned {
        uid: 0,
        gid: 0,
        mode: 0o755,
    };
    let own = Owned {
        uid: UID,
        gid: 100,
        mode: 0o755,
    };
    let refused = |step: DirectoryStep| matches!(step, DirectoryStep::Refuse(_));
    /// A case's name, the entry at the path, its parent's entry, the wanted owner, and whether
    /// the planned step is the expected one.
    type Case<'a> = (
        &'a str,
        PathEntry,
        &'a PathEntry,
        Owned,
        fn(&DirectoryStep) -> bool,
    );
    let cases: [Case<'_>; 12] = [
        (
            "a missing root ancestor is made by root",
            PathEntry::Missing,
            &root_parent,
            root,
            |step| *step == DirectoryStep::Create { privileged: true },
        ),
        (
            "root's own ancestor is taken as found",
            directory(0, 0o711, false, None),
            &root_parent,
            root,
            |step| *step == DirectoryStep::Current,
        ),
        (
            "a group-writable ancestor is refused",
            directory(0, 0o775, false, None),
            &root_parent,
            root,
            |step| matches!(step, DirectoryStep::Refuse(_)),
        ),
        (
            "another user's ancestor is refused",
            directory(UID, 0o755, false, None),
            &root_parent,
            root,
            |step| matches!(step, DirectoryStep::Refuse(_)),
        ),
        (
            "the owner's directory under root's needs root",
            PathEntry::Missing,
            &root_parent,
            own,
            |step| *step == DirectoryStep::Create { privileged: true },
        ),
        (
            "the owner makes their own under their own",
            PathEntry::Missing,
            &own_parent,
            own,
            |step| *step == DirectoryStep::Create { privileged: false },
        ),
        (
            "under a parent that is not there yet it needs root",
            PathEntry::Missing,
            &PathEntry::Missing,
            own,
            |step| *step == DirectoryStep::Create { privileged: true },
        ),
        (
            "the owner's with its mode is current",
            directory(UID, 0o755, false, None),
            &root_parent,
            own,
            |step| *step == DirectoryStep::Current,
        ),
        (
            "the owner's with another mode is the owner's to repair",
            directory(UID, 0o700, true, None),
            &root_parent,
            own,
            |step| *step == DirectoryStep::Repair { privileged: false },
        ),
        (
            "root's empty directory is never adopted",
            directory(0, 0o755, true, None),
            &root_parent,
            own,
            |step| matches!(step, DirectoryStep::Refuse(reason) if reason.contains("not mounted")),
        ),
        (
            "another user's directory is refused",
            directory(UID + 1, 0o755, true, None),
            &root_parent,
            own,
            |step| matches!(step, DirectoryStep::Refuse(_)),
        ),
        (
            "a link is refused",
            PathEntry::Other("a symbolic link"),
            &root_parent,
            own,
            |step| matches!(step, DirectoryStep::Refuse(reason) if reason.contains("symbolic link")),
        ),
    ];
    for (case, entry, parent, want, expected) in cases {
        let step = owned_directory_step(&entry, parent, want, UID);
        assert!(expected(&step), "{case}: {step:?}");
    }
    // Root provisioning for itself needs no authorization at all.
    assert_eq!(
        owned_directory_step(&PathEntry::Missing, &root_parent, root, 0),
        DirectoryStep::Create { privileged: false }
    );
    assert!(refused(owned_directory_step(
        &PathEntry::Other("not a directory"),
        &root_parent,
        root,
        0
    )));
}

#[test]
fn a_chain_under_a_missing_ancestor_is_judged_as_each_step_will_find_it() {
    let chain = plan_directory(
        Path::new(STORE),
        VolumeRole::Store,
        ExistingDirectory::Unpublished,
        OWNER,
    );
    let mut host = Host::bare();
    let fresh = directory_changes(&chain, UID, |path| host.entry(path)).unwrap();
    assert_eq!(
        fresh,
        DirectoryChanges {
            any: true,
            privileged: true
        }
    );

    // The same store path below the user's own tree needs nobody's authorization.
    let own = mount_root_operations(Path::new(HOME), OWNER);
    assert_eq!(
        directory_changes(&own, UID, |path| host.entry(path)).unwrap(),
        DirectoryChanges {
            any: true,
            privileged: false
        }
    );
    host.at("/home/dev/.cowshed", directory(UID, 0o700, false, None));
    host.at("/home/dev/.cowshed/mnt", directory(UID, 0o700, true, None));
    assert_eq!(
        directory_changes(&own, UID, |path| host.entry(path)).unwrap(),
        DirectoryChanges::default()
    );

    host.at("/private", directory(UID + 1, 0o755, false, None));
    assert!(matches!(
        directory_changes(&chain, UID, |path| host.entry(path)),
        Err(NativeBootstrapError::ForeignHostPath { path, .. }) if path == Path::new("/private")
    ));
}

#[test]
fn a_directory_store_path_is_unpublished_published_or_refused() {
    let path = Path::new(STORE);
    let ours = marker(VolumeRole::Store, SubstrateKind::Directory);
    let cases: [(&str, PathEntry, Option<ExistingDirectory>); 8] = [
        (
            "no entry",
            PathEntry::Missing,
            Some(ExistingDirectory::Unpublished),
        ),
        (
            "the owner's empty directory",
            directory(UID, 0o755, true, None),
            Some(ExistingDirectory::Unpublished),
        ),
        (
            "root's empty directory is the provisioning's to judge",
            directory(0, 0o755, true, None),
            Some(ExistingDirectory::Unpublished),
        ),
        (
            "the owner's directory with its marker",
            directory(UID, 0o755, false, Some(ours.clone())),
            Some(ExistingDirectory::Published),
        ),
        (
            "a marker of another role",
            directory(
                UID,
                0o755,
                false,
                Some(marker(VolumeRole::BuildCache, SubstrateKind::Directory)),
            ),
            None,
        ),
        (
            "a marker of another substrate",
            directory(
                UID,
                0o755,
                false,
                Some(marker(VolumeRole::Store, SubstrateKind::Zfs)),
            ),
            None,
        ),
        (
            "entries without a marker",
            directory(UID, 0o755, false, None),
            None,
        ),
        (
            "another user's published store",
            directory(UID + 1, 0o755, false, Some(ours)),
            None,
        ),
    ];
    for (case, entry, expected) in cases {
        let classified = classify_directory(path, VolumeRole::Store, &entry, OWNER);
        match expected {
            Some(state) => assert_eq!(classified.unwrap(), state, "{case}"),
            None => assert!(
                matches!(
                    classified,
                    Err(NativeBootstrapError::ForeignHostPath { .. })
                ),
                "{case}"
            ),
        }
    }
}

#[test]
fn zfs_get_output_reads_as_a_dataset_with_its_own_owner_and_marker() {
    let read = |owner: &str, role: &str, version: &str| {
        parse_dataset(&format!(
            "guid\t8569747932849924998\t-\nused\t98304\t-\ncowshed:owner\t{owner}\ncowshed:role\t{role}\ncowshed:version\t{version}\n"
        ))
    };
    let store = read("1000\tlocal", "store\tlocal", "2\tlocal").unwrap();
    assert_eq!(store.guid, "8569747932849924998");
    assert_eq!(store.used, 98_304);
    assert_eq!(store.owner.as_deref(), Some("1000"));
    assert_eq!(
        store.marker,
        ExistingMarkerEvidence::Valid(VolumeMarker::new(VolumeRole::Store, SubstrateKind::Zfs))
    );
    let cases = [
        (
            "an inherited owner authorizes nothing",
            read("1000\tinherited from tank", "-\t-", "-\t-"),
            None,
            ExistingMarkerEvidence::Missing,
        ),
        (
            "an unset owner",
            read("-\t-", "-\t-", "-\t-"),
            None,
            ExistingMarkerEvidence::Missing,
        ),
        (
            "a role without a version",
            read("-\t-", "store\tlocal", "-\t-"),
            None,
            ExistingMarkerEvidence::Invalid,
        ),
        (
            "an unknown role",
            read("-\t-", "vault\tlocal", "1\tlocal"),
            None,
            ExistingMarkerEvidence::Invalid,
        ),
        (
            "a version from the future",
            read("-\t-", "projects\tlocal", "9\tlocal"),
            None,
            ExistingMarkerEvidence::UnsupportedVersion(9),
        ),
    ];
    for (case, dataset, owner, marker) in cases {
        let dataset = dataset.unwrap();
        assert_eq!(dataset.owner, owner, "{case}");
        assert_eq!(dataset.marker, marker, "{case}");
    }
    assert!(parse_dataset("guid\t1\t-\n").is_err(), "no used");
    assert!(parse_dataset("guid 1\n").is_err(), "not tab separated");
}

#[test]
fn a_dataset_is_classified_from_the_kernel_mount_table_and_its_mountpoint() {
    let name = "tank/dev/cowshed/store";
    let path = Path::new(STORE);
    let root = mount(1, "/", "ext4", "/dev/vda1");
    let here = mount(2, STORE, "zfs", name);
    let existing = dataset(None, Some(VolumeRole::Store));
    let valid = directory(
        UID,
        0o755,
        false,
        Some(marker(VolumeRole::Store, SubstrateKind::Zfs)),
    );
    let empty = directory(0, 0o755, true, None);
    type Expected = fn(&Result<ExistingStorage, NativeBootstrapError>) -> bool;
    let foreign: Expected =
        |state| matches!(state, Err(NativeBootstrapError::ForeignHostPath { .. }));
    /// A case's name, the dataset, the mount table, the mountpoint's entry, and whether the
    /// classification is the expected one.
    type Case<'a> = (
        &'a str,
        Option<&'a Dataset>,
        Vec<Mount>,
        PathEntry,
        Expected,
    );
    let cases: [Case<'_>; 10] = [
        (
            "no dataset, no mountpoint",
            None,
            vec![root.clone()],
            PathEntry::Missing,
            |state| matches!(state, Ok(ExistingStorage::Absent)),
        ),
        (
            "no dataset, an empty mountpoint",
            None,
            vec![root.clone()],
            empty.clone(),
            |state| matches!(state, Ok(ExistingStorage::Absent)),
        ),
        (
            "no dataset, but the mountpoint holds entries",
            None,
            vec![root.clone()],
            directory(UID, 0o755, false, None),
            foreign,
        ),
        (
            "no dataset, but something is mounted there",
            None,
            vec![root.clone(), mount(2, STORE, "ext4", "/dev/sdb1")],
            empty.clone(),
            foreign,
        ),
        (
            "mounted there with its marker",
            Some(&existing),
            vec![root.clone(), here.clone()],
            valid,
            |state| matches!(state, Ok(ExistingStorage::MountedValid { exact_identifier }) if exact_identifier == "tank/dev/cowshed/store"),
        ),
        (
            "mounted there, empty, before its marker",
            Some(&existing),
            vec![root.clone(), here.clone()],
            directory(UID, 0o755, true, None),
            |state| matches!(state, Ok(ExistingStorage::MountedIncomplete { .. })),
        ),
        (
            "mounted there holding entries but no marker",
            Some(&existing),
            vec![root.clone(), here.clone()],
            directory(UID, 0o755, false, None),
            foreign,
        ),
        (
            "mounted there under a directory-store marker",
            Some(&existing),
            vec![root.clone(), here],
            directory(
                UID,
                0o755,
                false,
                Some(marker(VolumeRole::Store, SubstrateKind::Directory)),
            ),
            foreign,
        ),
        (
            "mounted somewhere else",
            Some(&existing),
            vec![root.clone(), mount(2, "/mnt/elsewhere", "zfs", name)],
            PathEntry::Missing,
            foreign,
        ),
        (
            "existing and unmounted",
            Some(&existing),
            vec![root],
            empty,
            |state| {
                matches!(
                    state,
                    Ok(ExistingStorage::ExistingUnmounted {
                        marker: ExistingMarkerEvidence::Valid(_),
                        ..
                    })
                )
            },
        ),
    ];
    for (case, dataset, mounts, entry, expected) in cases {
        let state = classify_dataset(name, path, VolumeRole::Store, dataset, &mounts, &entry);
        assert!(expected(&state), "{case}: {state:?}");
    }
}

#[test]
fn the_nearest_root_that_names_the_caller_serves_a_dataset_chain() {
    let owners = BTreeMap::from([
        ("tank/cowshed", "1000"),
        ("tank/home/cowshed", "2000"),
        ("tank/home/dev/zoned/cowshed", "1000"),
    ]);
    let serving = |dataset: &str, uid: u32| {
        serving_root_parent(dataset, uid, |root| {
            Ok(owners.get(root).map(|owner| (*owner).to_owned()))
        })
        .unwrap()
    };
    for (dataset, uid, parent) in [
        ("tank/home/dev/zoned", 1000, Some("tank/home/dev/zoned")),
        ("tank/home/dev", 1000, Some("tank")),
        ("tank/home/dev", 2000, Some("tank/home")),
        ("tank", 1000, Some("tank")),
        ("tank/home/dev", 3000, None),
        ("other/home", 1000, None),
    ] {
        assert_eq!(
            serving(dataset, uid).as_deref(),
            parent,
            "{dataset} for {uid}"
        );
    }
}

#[test]
fn a_published_store_decides_the_substrate_and_a_conflicting_declaration_is_refused() {
    let store = Path::new(STORE);
    let ext4 = [mount(1, "/", "ext4", "/dev/vda1")];
    let zfs = [
        mount(1, "/", "ext4", "/dev/vda1"),
        mount(2, STORE, "zfs", "tank/dev/cowshed/store"),
    ];
    let published = directory(
        UID,
        0o755,
        false,
        Some(marker(VolumeRole::Store, SubstrateKind::Directory)),
    );
    assert_eq!(
        existing_store(&ext4, store, &PathEntry::Missing),
        ExistingStore::None
    );
    assert_eq!(
        existing_store(&ext4, store, &directory(UID, 0o755, true, None)),
        ExistingStore::None
    );
    let directory_store = existing_store(&ext4, store, &published);
    assert_eq!(
        directory_store,
        ExistingStore::Directory {
            fs_type: "ext4".to_owned()
        }
    );
    let dataset_store = existing_store(&zfs, store, &directory(UID, 0o755, false, None));
    assert_eq!(
        dataset_store,
        ExistingStore::Zfs {
            parent: "tank/dev".to_owned(),
            dataset: "tank/dev/cowshed/store".to_owned()
        }
    );
    // Some other dataset mounted there is not a cowshed store.
    let stray = [mount(2, STORE, "zfs", "tank/scratch")];
    assert_eq!(
        existing_store(&stray, store, &directory(UID, 0o755, true, None)),
        ExistingStore::None
    );

    let same = zfs_config("tank/dev");
    let other = zfs_config("tank/elsewhere");
    assert!(
        existing_anchor(&ExistingStore::None, store, Some(&other))
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        existing_anchor(&directory_store, store, None).unwrap(),
        Some(StatFsEvidence::Other { fs_type }) if fs_type == "ext4"
    ));
    assert!(matches!(
        existing_anchor(&directory_store, store, Some(&same)),
        Err(NativeBootstrapError::SubstrateConflict { configured, .. }) if configured == "tank/dev"
    ));
    for configured in [None, Some(&same)] {
        assert!(matches!(
            existing_anchor(&dataset_store, store, configured).unwrap(),
            Some(StatFsEvidence::Zfs { containing_dataset: Some(dataset) })
                if dataset.root_parent() == Some("tank/dev")
        ));
    }
    assert!(matches!(
        existing_anchor(&dataset_store, store, Some(&other)),
        Err(NativeBootstrapError::SubstrateConflict { existing, configured })
            if existing.contains("tank/dev/cowshed/store") && configured == "tank/elsewhere"
    ));
}

#[test]
fn a_fresh_host_gets_a_directory_store_through_one_authorization() {
    let mut host = Host::bare();
    let setup = prepare_setup(&mut host, Path::new(HOME), None, CachesVolume::Keep).unwrap();
    assert!(setup.requires_authorization);
    let actions: Vec<_> = setup.steps.iter().map(|step| step.action.clone()).collect();
    assert_eq!(
        actions,
        [
            HostAction::CreateVolume {
                name: STORE.to_owned(),
                container: "/private/cowshed".to_owned(),
                mount_at: PathBuf::from(STORE),
            },
            HostAction::CreateVolume {
                name: BUILD_CACHE.to_owned(),
                container: "/private/cowshed".to_owned(),
                mount_at: PathBuf::from(BUILD_CACHE),
            },
            HostAction::EnsureMountRoot {
                path: PathBuf::from("/home/dev/.cowshed/mnt"),
                uid: UID,
                gid: 100,
            },
        ]
    );
    let owned: Vec<_> = setup.steps[0]
        .operations
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::EnsureOwnedDirectory {
                path, uid, mode, ..
            } => Some((path.display().to_string(), *uid, *mode)),
            _ => None,
        })
        .collect();
    assert_eq!(
        owned,
        [
            ("/private".to_owned(), 0, 0o755),
            ("/private/cowshed".to_owned(), 0, 0o755),
            (STORE.to_owned(), UID, 0o755),
        ]
    );
    assert!(matches!(
        setup.steps[0].operations.last(),
        Some(HostOperation::WriteMarkerAtomic { path, marker })
            if path == Path::new("/private/cowshed/store/.cowshed-volume.json")
                && marker.substrate() == SubstrateKind::Directory
                && marker.role() == VolumeRole::Store
    ));
    assert_eq!(setup.steps[2].then, Then::PublishHostConfig);
    assert!(
        setup
            .volumes
            .iter()
            .all(|volume| volume.state_before == VolumeState::Absent && volume.action == "created")
    );
}

#[test]
fn a_provisioned_directory_host_plans_nothing_and_validates_by_guards_alone() {
    let mut host = Host::bare();
    host.config_missing = false;
    host.at("/private", directory(0, 0o755, false, None))
        .at("/private/cowshed", directory(0, 0o755, false, None))
        .at(
            STORE,
            directory(
                UID,
                0o755,
                false,
                Some(marker(VolumeRole::Store, SubstrateKind::Directory)),
            ),
        )
        .at(
            BUILD_CACHE,
            directory(
                UID,
                0o755,
                false,
                Some(marker(VolumeRole::BuildCache, SubstrateKind::Directory)),
            ),
        )
        .at("/home/dev/.cowshed", directory(UID, 0o700, false, None))
        .at("/home/dev/.cowshed/mnt", directory(UID, 0o700, true, None));
    let setup = prepare_setup(&mut host, Path::new(HOME), None, CachesVolume::Retire).unwrap();
    assert!(setup.steps.is_empty(), "{:?}", setup.steps);
    assert!(!setup.requires_authorization);

    let gathered = gather(
        &mut host,
        Scope::HostStorage,
        Path::new(HOME),
        Path::new(HOME),
        None,
    )
    .unwrap();
    assert!(required_setup_actions(&gathered.plan).is_empty());
    assert_eq!(
        gathered.volumes.len(),
        1,
        "only setup reads the build cache"
    );

    // A store that is not there yet is setup's to make, named for the operator.
    let actions = required_setup_actions(
        &gather(
            &mut Host::bare(),
            Scope::HostStorage,
            Path::new(HOME),
            Path::new(HOME),
            None,
        )
        .unwrap()
        .plan,
    );
    assert_eq!(
        actions,
        [
            "provision owned directory /private",
            "provision owned directory /private/cowshed",
            "provision owned directory /private/cowshed/store",
            "write volume marker /private/cowshed/store/.cowshed-volume.json",
        ]
    );
}

#[test]
fn a_directory_store_and_its_mount_root_must_share_one_mount() {
    let mut host = Host::bare();
    host.mounted(2, "/home", "ext4", "/dev/vdb1");
    assert!(matches!(
        prepare_setup(&mut host, Path::new(HOME), None, CachesVolume::Keep),
        Err(NativeBootstrapError::SplitDirectoryStore { store_mount, mount_root_mount, .. })
            if store_mount == Path::new("/") && mount_root_mount == Path::new("/home")
    ));
}

/// A root `tank/dev/cowshed` that names the caller, declared by the checkout setup runs in.
fn zfs_host() -> Host {
    let mut host = Host::bare();
    host.with("tank/dev/cowshed", dataset(Some("1000"), None));
    host
}

#[test]
fn a_declared_zfs_root_gets_its_datasets_through_the_helper_alone() {
    let mut host = zfs_host();
    let configured = zfs_config("tank/dev");
    let setup = prepare_setup(
        &mut host,
        Path::new(HOME),
        Some(&configured),
        CachesVolume::Keep,
    )
    .unwrap();
    assert!(
        !setup.requires_authorization,
        "datasets are the helper's; no directory needs sudo"
    );
    let names: Vec<_> = setup
        .steps
        .iter()
        .map(|step| match &step.action {
            HostAction::CreateVolume {
                name, container, ..
            } => format!("create {name} in {container}"),
            HostAction::EnsureMountRoot { path, .. } => format!("mount root {}", path.display()),
            other => format!("{other:?}"),
        })
        .collect();
    assert_eq!(
        names,
        [
            "create tank/dev/cowshed/store in tank/dev/cowshed",
            "create tank/dev/cowshed/projects in tank/dev/cowshed",
            "create tank/dev/cowshed/build-cache in tank/dev/cowshed",
            "mount root /home/dev/.cowshed/mnt",
        ]
    );
    let commands: Vec<_> = setup
        .steps
        .iter()
        .flat_map(|step| command_lines(&step.operations))
        .collect();
    assert_eq!(
        commands,
        [
            "cowshed-helper tank/dev/cowshed create tank/dev/cowshed/store mountpoint=/private/cowshed/store cowshed:version=2 cowshed:role=store",
            "cowshed-helper tank/dev/cowshed create tank/dev/cowshed/projects mountpoint=none cowshed:version=1 cowshed:role=projects",
            "cowshed-helper tank/dev/cowshed create tank/dev/cowshed/build-cache mountpoint=/private/cowshed/build-cache cowshed:version=1 cowshed:role=build-cache",
        ]
    );
    assert!(matches!(
        setup.steps[0].operations.first(),
        Some(HostOperation::VerifyZfsDelegation { parent, required_root })
            if parent == "tank/dev" && required_root == "tank/dev/cowshed"
    ));
}

#[test]
fn an_existing_zfs_store_is_mounted_repaired_or_left_as_its_state_says() {
    let store = "tank/dev/cowshed/store";
    let configured = zfs_config("tank/dev");
    let mut host = zfs_host();
    host.with(store, dataset(None, Some(VolumeRole::Store)))
        .with(
            "tank/dev/cowshed/projects",
            dataset(None, Some(VolumeRole::Projects)),
        );
    let detached = prepare_setup(
        &mut host,
        Path::new(HOME),
        Some(&configured),
        CachesVolume::Keep,
    )
    .unwrap();
    assert_eq!(
        detached.steps[0].action,
        HostAction::MountExisting {
            name: store.to_owned(),
            uuid: "4242".to_owned(),
            size_bytes: 98_304,
            mount_at: PathBuf::from(STORE),
        }
    );
    assert_eq!(
        command_lines(&detached.steps[0].operations),
        [
            "cowshed-helper tank/dev/cowshed set tank/dev/cowshed/store mountpoint=/private/cowshed/store",
            "cowshed-helper tank/dev/cowshed mount tank/dev/cowshed/store",
        ]
    );

    // Mounted and empty: only its marker is missing.
    host.mounted(2, STORE, "zfs", store)
        .at(STORE, directory(UID, 0o755, true, None));
    let incomplete = prepare_setup(&mut host, Path::new(HOME), None, CachesVolume::Keep).unwrap();
    assert!(matches!(
        &incomplete.steps[0].action,
        HostAction::RepairMounted { name, .. } if name == store
    ));
    assert_eq!(
        command_lines(&incomplete.steps[0].operations),
        [
            "cowshed-helper tank/dev/cowshed set tank/dev/cowshed/store cowshed:version=2 cowshed:role=store"
        ]
    );

    // Published: the mounted store selects its own root, with no declaration at hand.
    host.at(
        STORE,
        directory(
            UID,
            0o755,
            false,
            Some(marker(VolumeRole::Store, SubstrateKind::Zfs)),
        ),
    );
    let gathered = gather(
        &mut host,
        Scope::HostStorage,
        Path::new(HOME),
        Path::new(HOME),
        None,
    )
    .unwrap();
    assert!(required_setup_actions(&gathered.plan).is_empty());
    assert!(matches!(
        gathered.plan.substrate(),
        SelectedSubstrate::Zfs { parent, .. } if parent == "tank/dev"
    ));
}

#[test]
fn a_zfs_root_that_does_not_exist_is_the_administrators_to_create() {
    let mut host = Host::bare();
    let configured = zfs_config("tank/dev");
    let error = prepare_setup(
        &mut host,
        Path::new(HOME),
        Some(&configured),
        CachesVolume::Keep,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("zfs create -o mountpoint=none -o cowshed:owner=<uid> tank/dev/cowshed"),
        "{error}"
    );
}

#[test]
fn the_retired_caches_dataset_is_deleted_only_mounted_and_empty() {
    let caches = "tank/dev/cowshed/caches";
    let configured = zfs_config("tank/dev");
    let mut host = zfs_host();
    host.with(caches, dataset(None, Some(VolumeRole::Caches)))
        .mounted(3, "/private/cowshed/caches", "zfs", caches)
        .at(
            "/private/cowshed/caches",
            directory(
                UID,
                0o755,
                false,
                Some(marker(VolumeRole::Caches, SubstrateKind::Zfs)),
            ),
        );
    let retire = |host: &mut Host| {
        prepare_setup(
            host,
            Path::new(HOME),
            Some(&configured),
            CachesVolume::Retire,
        )
    };
    let setup = retire(&mut host).unwrap();
    assert_eq!(
        setup.steps[0].action,
        HostAction::DeleteVolume {
            name: caches.to_owned(),
            uuid: "4242".to_owned(),
            mounted_at: PathBuf::from("/private/cowshed/caches"),
        }
    );
    assert_eq!(
        command_lines(&setup.steps[0].operations),
        [
            "cowshed-helper tank/dev/cowshed unmount tank/dev/cowshed/caches",
            "cowshed-helper tank/dev/cowshed destroy tank/dev/cowshed/caches",
        ]
    );
    assert!(
        setup
            .volumes
            .iter()
            .all(|volume| volume.role != VolumeRole::Caches)
    );

    host.only_marker = false;
    assert!(matches!(
        retire(&mut host),
        Err(NativeBootstrapError::CachesVolumeNotRetirable(_))
    ));
    // Kept, a mounted one plans nothing.
    let kept = prepare_setup(
        &mut host,
        Path::new(HOME),
        Some(&configured),
        CachesVolume::Keep,
    )
    .unwrap();
    assert!(kept.volumes.iter().any(|volume| {
        volume.role == VolumeRole::Caches && volume.state_before == VolumeState::MountedValid
    }));
}

#[test]
fn the_helper_is_found_beside_the_executable_first_and_run_as_root_one_of_two_ways() {
    let path = OsStr::new("/run/wrappers/bin:relative/bin:/usr/bin");
    let on = |present: &'static [&'static str]| {
        move |candidate: &Path| present.iter().any(|path| Path::new(path) == candidate)
    };
    for (beside, present, found) in [
        (
            Some("/opt/cowshed/bin"),
            &["/opt/cowshed/bin/cowshed-helper", "/usr/bin/cowshed-helper"][..],
            Some("/opt/cowshed/bin/cowshed-helper"),
        ),
        (
            Some("/opt/cowshed/bin"),
            &["/usr/bin/cowshed-helper"][..],
            Some("/usr/bin/cowshed-helper"),
        ),
        (
            None,
            &[
                "/run/wrappers/bin/cowshed-helper",
                "/usr/bin/cowshed-helper",
            ][..],
            Some("/run/wrappers/bin/cowshed-helper"),
        ),
        (None, &["relative/bin/cowshed-helper"][..], None),
        (Some("/opt/cowshed/bin"), &[][..], None),
    ] {
        assert_eq!(
            locate_helper(beside.map(Path::new), path, on(present)).as_deref(),
            found.map(Path::new),
            "{beside:?} {present:?}"
        );
    }
    let arguments = [
        "tank/cowshed".to_owned(),
        "mount".to_owned(),
        "tank/cowshed/store".to_owned(),
    ];
    let helper = Path::new("/run/wrappers/bin/cowshed-helper");
    assert_eq!(
        helper_invocation(true, helper, &arguments),
        (
            OsString::from("/run/wrappers/bin/cowshed-helper"),
            vec![
                "tank/cowshed".into(),
                "mount".into(),
                "tank/cowshed/store".into()
            ]
        )
    );
    assert_eq!(
        helper_invocation(false, helper, &arguments),
        (
            OsString::from("sudo"),
            vec![
                "-n".into(),
                "--".into(),
                "/run/wrappers/bin/cowshed-helper".into(),
                "tank/cowshed".into(),
                "mount".into(),
                "tank/cowshed/store".into()
            ]
        )
    );
}

#[test]
fn only_a_privileged_directory_step_becomes_a_root_command() {
    let want = Owned {
        uid: UID,
        gid: 100,
        mode: 0o755,
    };
    assert_eq!(
        privileged_directory_command(&DirectoryStep::Create { privileged: true }, STORE, want),
        Some((
            "install",
            ["-d", "-m", "0755", "-o", "1000", "-g", "100", "--", STORE]
                .map(str::to_owned)
                .to_vec()
        ))
    );
    assert_eq!(
        privileged_directory_command(&DirectoryStep::Repair { privileged: true }, STORE, want),
        Some(("chmod", ["0755", "--", STORE].map(str::to_owned).to_vec()))
    );
    for step in [
        DirectoryStep::Current,
        DirectoryStep::Create { privileged: false },
        DirectoryStep::Repair { privileged: false },
        DirectoryStep::Refuse("it is a symbolic link".to_owned()),
    ] {
        assert_eq!(
            privileged_directory_command(&step, STORE, want),
            None,
            "{step:?}"
        );
    }
}

#[test]
fn a_substrate_declaration_is_read_from_the_nearest_cowshed_toml_above() {
    let root = std::env::temp_dir().join(format!(
        "cowshed-linux-bootstrap-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let nested = root.join("checkout/packages/app");
    fs::create_dir_all(&nested).unwrap();
    assert_eq!(declared_substrate(&root).unwrap(), None, "no file");
    fs::write(
        root.join("checkout/.cowshed.toml"),
        "[substrate]\nkind = \"zfs\"\nparent = \"tank/dev\"\n",
    )
    .unwrap();
    assert_eq!(
        declared_substrate_above(&nested)
            .unwrap()
            .as_ref()
            .map(SubstrateConfig::parent),
        Some("tank/dev")
    );
    // The nearest file decides, even when it declares no substrate.
    fs::write(nested.join(".cowshed.toml"), "[sandbox]\ndeny = []\n").unwrap();
    assert_eq!(declared_substrate_above(&nested).unwrap(), None);
    fs::write(
        nested.join(".cowshed.toml"),
        "[substrate]\nkind = \"zfs\"\n",
    )
    .unwrap();
    assert!(matches!(
        declared_substrate_above(&nested),
        Err(NativeBootstrapError::SubstrateDeclaration { path, .. })
            if path == nested.join(".cowshed.toml")
    ));
    fs::remove_dir_all(&root).unwrap();
}
