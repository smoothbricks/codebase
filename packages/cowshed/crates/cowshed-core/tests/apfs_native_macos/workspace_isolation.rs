use super::*;

/// A published session image `workspace` with its own record, CA key and incarnation.
fn published_session(layout: &StorageLayout, workspace: &str, incarnation: &str) -> PathBuf {
    let image = layout
        .session_image(&WorkspaceName::session(workspace).expect("session"))
        .expect("session image")
        .image()
        .to_owned();
    create_image(&image);
    write_session_metadata(&image, workspace, incarnation);
    image
}

/// Canonical unlink is the retirement commit for clean, recoverable source images. A real
/// immutable-file refusal must keep its authority, and retry must release bytes without trash.
#[test]
fn clean_source_release_unlinks_physically_and_keeps_authority_when_unlink_refuses() {
    const LIVE: &str = "00000000000000000000000000000024";
    let fixture = Fixture::new("direct-source-release");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let canonical = published_session(&layout, "raven", LIVE);
    let workspace = host
        .list(&repo())
        .expect("inventory")
        .into_iter()
        .find(|fact| fact.workspace.name().as_str() == "raven")
        .expect("raven")
        .workspace;
    let retired = RetiredRef::new(workspace, Revision::new(1));
    let record = std::fs::read(sidecar_path(&canonical)).expect("record");
    let key = std::fs::read(ca_key_path(&canonical)).expect("key");
    struct MutableOnDrop(PathBuf);
    impl Drop for MutableOnDrop {
        fn drop(&mut self) {
            if self.0.exists() {
                set_file_flags(&self.0, 0);
            }
        }
    }
    let flags = MutableOnDrop(canonical.clone());
    set_file_flags(&canonical, libc::UF_IMMUTABLE);
    let error = host
        .release_workspace(&config, &retired)
        .expect_err("unlink refuses");
    assert!(
        canonical.exists(),
        "failed unlink must preserve the source image: {error}"
    );
    assert_eq!(std::fs::read(sidecar_path(&canonical)).unwrap(), record);
    assert_eq!(std::fs::read(ca_key_path(&canonical)).unwrap(), key);
    assert!(!layout.project().sessions.join(".trash").exists());
    set_file_flags(&canonical, 0);
    drop(flags);
    host.release_workspace(&config, &retired)
        .expect("retry physically releases");
    assert!(!canonical.exists());
    assert!(!sidecar_path(&canonical).exists());
    assert!(!ca_key_path(&canonical).exists());
    assert!(!layout.project().sessions.join(".trash").exists());
}

/// The state a retirement leaves when it crashes right after its commit: the record and key are
/// linked at the trash names, the image renamed onto the trash, and the canonical names not yet
/// unlinked.
fn retire_and_crash_after_commit(canonical: &Path, trash: &Path) {
    std::fs::create_dir_all(trash.parent().expect("trash parent")).expect("trash directory");
    std::fs::hard_link(sidecar_path(canonical), sidecar_path(trash)).expect("link record");
    std::fs::hard_link(ca_key_path(canonical), ca_key_path(trash)).expect("link key");
    std::fs::rename(canonical, trash).expect("commit the image rename");
}

/// The B20 failure: one workspace's read failed because a sibling's record was missing or
/// corrupt. A named snapshot reads the named workspace's own record, mount and checkpoints only.
#[test]
fn named_snapshot_reads_only_its_own_record_beside_broken_siblings() {
    const OWN: &str = "00000000000000000000000000000011";
    let fixture = Fixture::new("named-snapshot");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let name = WorkspaceName::session("raven").expect("session");
    let own = published_session(&layout, "raven", OWN);
    let label = CheckpointLabel::new("ready").expect("label");
    let checkpoint = layout
        .checkpoint_image(&name, &label)
        .expect("checkpoint")
        .image()
        .to_owned();
    create_image(&checkpoint);
    write_session_metadata(&checkpoint, "raven", OWN);
    host.publish_checkpoint_fact(&checkpoint, &label, Revision::new(3), Pin::Pinned)
        .expect("checkpoint fact");

    let missing = published_session(&layout, "missing", "00000000000000000000000000000012");
    std::fs::remove_file(sidecar_path(&missing)).expect("lose the sibling's record");
    let corrupt_name = WorkspaceName::session("corrupt").expect("session");
    let corrupt = published_session(&layout, "corrupt", "00000000000000000000000000000013");
    std::fs::write(sidecar_path(&corrupt), b"{ not a record").expect("corrupt sibling record");
    let corrupt_checkpoint = layout
        .checkpoint_image(&corrupt_name, &label)
        .expect("sibling checkpoint")
        .image()
        .to_owned();
    create_image(&corrupt_checkpoint);
    std::fs::write(checkpoint_fact_path(&corrupt_checkpoint), b"{ not a fact")
        .expect("corrupt sibling checkpoint fact");

    // Control: the project-wide reads visit every sibling and fail closed on the corrupt ones.
    assert!(host.list(&repo()).is_err());
    assert!(host.checkpoints(&repo()).is_err());

    let (derived, metadata) = host
        .workspace_snapshot(&repo(), &name)
        .expect("a named snapshot never reads a sibling")
        .expect("raven is published");
    assert_eq!(derived.workspace.name(), &name);
    assert_eq!(derived.workspace.incarnation().as_str(), OWN);
    assert_eq!(derived.mount_state, MountState::Detached);
    assert_eq!(
        derived
            .checkpoints
            .iter()
            .map(|checkpoint| (
                checkpoint.label.as_str(),
                checkpoint.revision,
                checkpoint.pin
            ))
            .collect::<Vec<_>>(),
        vec![("ready", Revision::new(3), Pin::Pinned)]
    );
    assert_eq!(
        metadata,
        DetachedWorkspaceMetadata::read_for_image(&own).expect("own record")
    );
    assert_eq!(
        host.canonical_metadata(&repo(), &name)
            .expect("own record alone")
            .expect("present"),
        metadata
    );
    assert_eq!(
        host.workspace_mount_state(&derived.workspace)
            .expect("own mount state"),
        MountState::Detached
    );

    // The broken siblings fail closed when they are the ones named, never silently absent.
    let missing_name = WorkspaceName::session("missing").expect("session");
    assert!(host.workspace_snapshot(&repo(), &missing_name).is_err());
    assert!(host.canonical_metadata(&repo(), &missing_name).is_err());
    assert!(host.workspace_snapshot(&repo(), &corrupt_name).is_err());
    assert!(host.canonical_metadata(&repo(), &corrupt_name).is_err());
    let absent = WorkspaceName::session("absent").expect("session");
    assert!(
        host.workspace_snapshot(&repo(), &absent)
            .expect("an unpublished name is a fact, not an error")
            .is_none()
    );
}

/// Retirement links a complete record and CA key at the trash names before the image rename that
/// commits it. A crash before the commit leaves a prepared record that authorizes nothing and a
/// retry replaces; a crash after it leaves canonical names that readers already treat as retired
/// and recovery unlinks; the removed session never reads as active again.
#[test]
fn retirement_publishes_a_complete_trash_record_before_its_image_commit() {
    const LIVE: &str = "00000000000000000000000000000021";
    const OTHER: &str = "00000000000000000000000000000022";
    let fixture = Fixture::new("atomic-retirement");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    create_image(layout.main_image().expect("main").image());
    let raven = WorkspaceName::session("raven").expect("session");
    let canonical = published_session(&layout, "raven", LIVE);
    let key = std::fs::read(ca_key_path(&canonical)).expect("canonical key");
    let trash_directory = layout.project().sessions.join(".trash");
    std::fs::create_dir_all(&trash_directory).expect("trash directory");
    let trash = trash_directory.join(format!("raven-{LIVE}.dmg"));
    let published = |host: &MacOsApfsExecutionHost<RecordingRunner>| {
        host.list(&repo())
            .expect("listing")
            .iter()
            .any(|fact| fact.workspace.name() == &raven)
    };

    // A trash name that is not the record's identity is refused before anything moves.
    let misnamed = trash_directory.join(format!("raven-{OTHER}.dmg"));
    assert!(matches!(
        host.retire_image(&canonical, &misnamed),
        Err(ApfsStorageError::MarkerMismatch(_))
    ));
    assert!(canonical.exists());
    assert!(!sidecar_path(&misnamed).exists());
    assert!(!ca_key_path(&misnamed).exists());

    // Crash before the commit: the record and key are linked at the trash names, the image is not.
    std::fs::hard_link(sidecar_path(&canonical), sidecar_path(&trash)).expect("prepare record");
    std::fs::hard_link(ca_key_path(&canonical), ca_key_path(&trash)).expect("prepare key");
    assert!(
        host.workspace_snapshot(&repo(), &raven)
            .expect("snapshot")
            .is_some()
    );
    assert!(published(&host));
    // A prepared record authorizes nothing beyond its own links: GC leaves the live workspace's
    // image, record and key exactly as they were.
    execute_gc(&host, &config).expect("gc beside a prepared retirement");
    assert!(!sidecar_path(&trash).exists());
    assert!(!ca_key_path(&trash).exists());
    assert!(canonical.exists());
    assert!(sidecar_path(&canonical).exists());
    assert_eq!(
        std::fs::read(ca_key_path(&canonical)).expect("live key"),
        key
    );
    assert!(
        host.workspace_snapshot(&repo(), &raven)
            .expect("snapshot")
            .is_some()
    );

    // Crash again before the commit, then the live record moves on (rewritten by rename).
    std::fs::hard_link(sidecar_path(&canonical), sidecar_path(&trash)).expect("prepare record");
    std::fs::hard_link(ca_key_path(&canonical), ca_key_path(&trash)).expect("prepare key");
    let mut current = DetachedWorkspaceMetadata::read_for_image(&canonical).expect("live record");
    current.updated_at = "2026-07-14T00:00:00Z".to_owned();
    current
        .write_for_image(&canonical)
        .expect("rewrite live record");

    // The retry discards the prepared record and retires under the current one.
    host.retire_image(&canonical, &trash)
        .expect("retry retires");
    assert!(!canonical.exists());
    assert!(!sidecar_path(&canonical).exists());
    assert!(!ca_key_path(&canonical).exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&trash).expect("trash record"),
        current
    );
    assert_eq!(std::fs::read(ca_key_path(&trash)).expect("trash key"), key);

    // The removed session is gone from every read.
    assert!(
        host.workspace_snapshot(&repo(), &raven)
            .expect("snapshot")
            .is_none()
    );
    assert!(
        host.canonical_metadata(&repo(), &raven)
            .expect("metadata")
            .is_none()
    );
    assert!(!published(&host));
    assert!(
        host.workspace_mount_state(&session_workspace("raven", LIVE))
            .is_err()
    );

    // A committed retirement is never retired over, and its record is left as it was.
    create_image(&canonical);
    write_session_metadata(&canonical, "raven", LIVE);
    assert!(host.retire_image(&canonical, &trash).is_err());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&trash).expect("trash record"),
        current
    );
    assert_eq!(std::fs::read(ca_key_path(&trash)).expect("trash key"), key);

    // Crash after the commit: readers already see the workspace retired, without an error.
    let kite = WorkspaceName::session("kite").expect("session");
    let kite_canonical = published_session(&layout, "kite", OTHER);
    let kite_trash = trash_directory.join(format!("kite-{OTHER}.dmg"));
    retire_and_crash_after_commit(&kite_canonical, &kite_trash);
    assert!(
        host.workspace_snapshot(&repo(), &kite)
            .expect("snapshot")
            .is_none()
    );
    assert!(
        host.canonical_metadata(&repo(), &kite)
            .expect("metadata")
            .is_none()
    );
    assert!(
        !host
            .list(&repo())
            .expect("listing")
            .iter()
            .any(|fact| fact.workspace.name() == &kite)
    );
    let kite_lock = layout.session_image(&kite).expect("kite").lock().to_owned();
    host.recover_pending(&config, std::slice::from_ref(&kite_lock))
        .expect("recovery finishes the committed retirement");
    assert!(!sidecar_path(&kite_canonical).exists());
    assert!(!ca_key_path(&kite_canonical).exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&kite_trash)
            .expect("kite trash record")
            .workspace_incarnation
            .as_str(),
        OTHER
    );
    assert!(ca_key_path(&kite_trash).exists());
}

/// A recovery pass run under workspace locks names those workspaces' canonical, staged and undo
/// records before reading any record, so a broken sibling or foreign project never fails it.
#[test]
fn locked_recovery_settles_only_its_own_workspace_and_never_reads_a_broken_neighbour() {
    const OWN: &str = "00000000000000000000000000000031";
    const SIBLING: &str = "00000000000000000000000000000032";
    const CORRUPT: &[u8] = b"{ corrupt";
    let fixture = Fixture::new("scoped-recovery");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let host = native_host(&fixture, RecordingRunner::default());

    let kite = WorkspaceName::session("kite").expect("session");
    let canonical = published_session(&layout, "kite", OWN);
    let trash = layout
        .project()
        .sessions
        .join(format!(".trash/kite-{OWN}.dmg"));
    retire_and_crash_after_commit(&canonical, &trash);

    let broken = WorkspaceName::session("broken").expect("session");
    let broken_canonical = layout
        .session_image(&broken)
        .expect("broken")
        .image()
        .to_owned();
    let staged = layout
        .project()
        .project_root
        .join(".staging")
        .join(format!("broken-{SIBLING}.dmg"));
    let undo = layout
        .project()
        .checkpoints
        .join("broken")
        .join(format!("pre-restore-{SIBLING}.dmg"));
    let foreign_layout = StorageLayout::new(
        &fixture.root,
        &RepoId::parse("other/thing").expect("foreign repo"),
    )
    .expect("foreign layout");
    adopt_owner(&foreign_layout);
    let foreign = foreign_layout
        .main_image()
        .expect("foreign main")
        .image()
        .to_owned();
    let poisoned = [
        sidecar_path(&broken_canonical),
        sidecar_path(&staged),
        sidecar_path(&undo),
        sidecar_path(&foreign),
    ];
    for record in &poisoned {
        std::fs::create_dir_all(record.parent().expect("record parent")).expect("record parent");
        std::fs::write(record, CORRUPT).expect("corrupt neighbour record");
    }
    // A neighbour's committed release that cannot even be opened: a pass that read it would
    // refuse, and one that freed it would block on a release that is not its own.
    let neighbour_release = broken_canonical
        .parent()
        .expect("sessions")
        .join(".releasing")
        .join(format!("broken-{SIBLING}.dmg"));
    std::fs::create_dir_all(neighbour_release.parent().expect("journal")).expect("journal");
    std::fs::write(&neighbour_release, CORRUPT).expect("neighbour release");
    std::fs::set_permissions(&neighbour_release, std::fs::Permissions::from_mode(0o000))
        .expect("unreadable neighbour release");

    let lock = layout.session_image(&kite).expect("kite").lock().to_owned();
    host.recover_pending(&config, std::slice::from_ref(&lock))
        .expect("a locked pass reads its own workspace only");
    assert!(!sidecar_path(&canonical).exists());
    assert!(!ca_key_path(&canonical).exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&trash)
            .expect("trash record")
            .workspace_incarnation
            .as_str(),
        OWN
    );
    assert!(ca_key_path(&trash).exists());
    for record in &poisoned {
        assert_eq!(
            std::fs::read(record).expect("neighbour record"),
            CORRUPT,
            "{} was touched",
            record.display()
        );
    }
    host.recover_pending(&config, std::slice::from_ref(&lock))
        .expect("repeated locked recovery is idempotent");
    assert_eq!(
        std::fs::metadata(&neighbour_release)
            .expect("the neighbour's release is left")
            .len(),
        u64::try_from(CORRUPT.len()).expect("length"),
        "a locked pass neither reads nor frees a neighbour's committed release"
    );
    let main_lock = layout.main_image().expect("main").lock().to_owned();
    host.recover_pending(&config, &[main_lock, lock])
        .expect("main and kite locks select main and kite only");

    // A path that is not a workspace lock is refused, never widened to the whole store.
    assert!(
        host.recover_pending(&config, std::slice::from_ref(&canonical))
            .is_err()
    );
    // Control: the store-wide pass of startup and maintenance reads every record and fails closed.
    assert!(host.recover_pending(&config, &[]).is_err());
}
