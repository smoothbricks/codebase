use super::*;
use crate::fork_lock::Spawn as _;
use crate::test_backstop::{ChildBackstop as _, FileBackstop as _};
use std::fs::{File, FileTimes};
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::process::Command;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "cowshed-reaper-test-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
    fn store(&self) -> PathBuf {
        self.0.join("store")
    }
    fn scratch(&self, name: &str, pid: u32) -> PathBuf {
        let path = self.0.join(name);
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        record_owners(&path, &[pid]).unwrap();
        fs::write(path.join("s.sock.lock"), b"").unwrap();
        path
    }
    fn reaper(&self) -> Reaper {
        Reaper::new([self.0.clone()]).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn dead_pid() -> u32 {
    let mut child = Command::new("/bin/sh")
        .args(["-c", "exit 0"])
        .spawn_locked()
        .unwrap();
    let pid = child.id();
    assert!(child.wait_bounded().unwrap().success());
    pid
}

const LIVE: i32 = 1001;
const DEAD: i32 = 1002;
/// An owner whose liveness cannot be read.
const MUTE: i32 = 1003;

/// What a staged look must have become.
type Oracle = fn(&Look<()>) -> bool;

/// A name alone passes another tool's entry, even with a cowshed record, and keeps the real-APFS
/// fixtures' for their own sweep; every other name is stat'ed next, cowshed's own as known.
#[test]
fn a_name_alone_passes_another_tools_entry_and_asks_for_the_rest() {
    for name in [
        "node_a",
        "bash_a",
        "cargo_a",
        "bun_a",
        "devenv-a",
        "sccache_a",
        "smoo-ram2-100",
    ] {
        let look = Look::<()>::name(name);
        assert!(
            matches!(look, Look::Verdict(Verdict::Pass)),
            "{name}: {look:?}"
        );
    }
    let look = Look::<()>::name("cowshed-itest-dead");
    assert!(
        matches!(look, Look::Verdict(Verdict::Keep(Keep::ApfsFixture))),
        "{look:?}"
    );
    for (name, known) in [
        ("cowshed-sock-dead", true),
        ("cs-trace", true),
        ("other-output", false),
    ] {
        let look = Look::<()>::name(name);
        assert!(
            matches!(look, Look::Metadata { known: asked } if asked == known),
            "{name}: {look:?}"
        );
    }
}

/// An entry is judged only as a private directory of this user, stat'ed without following a
/// link: anything else, or an entry that cannot be stat'ed, is kept when cowshed named it and
/// passed otherwise. Its owner record decides next: a missing record passes another name's entry
/// and keeps cowshed's, and a malformed one is an unknown owner, never a dead one.
#[test]
fn a_directory_is_private_only_for_its_owner_without_foreign_writers() {
    for (directory, owner, mode, reader, expected) in [
        (true, 501, 0o700, 501, true),
        (true, 501, 0o755, 501, true),
        (false, 501, 0o700, 501, false),
        (true, 502, 0o700, 501, false),
        (true, 501, 0o720, 501, false),
        (true, 501, 0o702, 501, false),
    ] {
        assert_eq!(
            private_directory(directory, owner, mode, reader),
            expected,
            "directory={directory} owner={owner} mode={mode:o} reader={reader}"
        );
    }
}

#[test]
fn an_entry_is_judged_only_as_a_private_directory_with_a_complete_record() {
    let not_found = || io::Error::from(io::ErrorKind::NotFound);
    let metadata: [(bool, io::Result<Option<()>>, Oracle); 6] = [
        (true, Ok(Some(())), |look| {
            matches!(look, Look::Owners { known: true, .. })
        }),
        (false, Ok(Some(())), |look| {
            matches!(look, Look::Owners { known: false, .. })
        }),
        (true, Ok(None), |look| {
            matches!(look, Look::Verdict(Verdict::Keep(Keep::NotPrivate)))
        }),
        (false, Ok(None), |look| {
            matches!(look, Look::Verdict(Verdict::Pass))
        }),
        (
            true,
            Err(not_found()),
            |look| matches!(look, Look::Verdict(Verdict::Keep(Keep::Uninspected(error))) if error.kind() == io::ErrorKind::NotFound),
        ),
        (false, Err(not_found()), |look| {
            matches!(look, Look::Verdict(Verdict::Pass))
        }),
    ];
    for (known, private, expected) in metadata {
        let look = Look::metadata(known, private);
        assert!(expected(&look), "known {known}: {look:?}");
    }
    let records: [(bool, io::Result<Vec<i32>>, Oracle); 5] = [
        (
            true,
            Ok(vec![DEAD, LIVE]),
            |look| matches!(look, Look::Liveness { owners, .. } if owners == &[DEAD, LIVE]),
        ),
        (
            false,
            Ok(vec![DEAD]),
            |look| matches!(look, Look::Liveness { owners, .. } if owners == &[DEAD]),
        ),
        (
            true,
            Err(not_found()),
            |look| matches!(look, Look::Verdict(Verdict::Keep(Keep::UnknownOwner(error))) if error.kind() == io::ErrorKind::NotFound),
        ),
        (false, Err(not_found()), |look| {
            matches!(look, Look::Verdict(Verdict::Pass))
        }),
        (
            false,
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown owner record format",
            )),
            |look| matches!(look, Look::Verdict(Verdict::Keep(Keep::UnknownOwner(error))) if error.kind() == io::ErrorKind::InvalidData),
        ),
    ];
    for (known, owners, expected) in records {
        let look = Look::owners(known, (), owners);
        assert!(expected(&look), "known {known}: {look:?}");
    }
}

/// Death, never age or a name, reclaims, and only when every recorded owner is observed dead:
/// the first owner observed live, or whose liveness cannot be read, keeps the entry naming it,
/// and no observed owner at all is an unknown owner.
#[test]
fn only_every_recorded_owner_observed_dead_reclaims() {
    let mute = || Err(io::Error::from_raw_os_error(libc::EPERM));
    let rows: [(Vec<(i32, io::Result<bool>)>, Oracle); 6] = [
        (vec![(DEAD, Ok(false))], |look| {
            matches!(look, Look::Verdict(Verdict::Reclaim(())))
        }),
        (vec![(DEAD, Ok(false)), (DEAD + 1, Ok(false))], |look| {
            matches!(look, Look::Verdict(Verdict::Reclaim(())))
        }),
        (vec![(LIVE, Ok(true))], |look| {
            matches!(look, Look::Verdict(Verdict::Keep(Keep::LiveOwner(LIVE))))
        }),
        // A detached writer still owns it after the parent that recorded it died.
        (vec![(DEAD, Ok(false)), (LIVE, Ok(true))], |look| {
            matches!(look, Look::Verdict(Verdict::Keep(Keep::LiveOwner(LIVE))))
        }),
        (
            vec![(DEAD, Ok(false)), (MUTE, mute()), (LIVE, Ok(true))],
            |look| {
                matches!(
                    look,
                    Look::Verdict(Verdict::Keep(Keep::UninspectableOwner(MUTE, _)))
                )
            },
        ),
        (
            Vec::new(),
            |look| matches!(look, Look::Verdict(Verdict::Keep(Keep::UnknownOwner(error))) if error.kind() == io::ErrorKind::InvalidData),
        ),
    ];
    for (observed, expected) in rows {
        let shown = format!("{observed:?}");
        let look = Look::liveness((), observed);
        assert!(expected(&look), "{shown}: {look:?}");
    }
}

/// An owner record names its pids only whole: the magic line, at least one positive pid, and its
/// end, within the record's bound. A torn or foreign record is an unknown owner, never a dead one.
#[test]
fn an_owner_record_names_its_pids_only_when_complete() {
    let oversized = format!("{MAGIC}\n{}end\n", "7\n".repeat(600));
    for (text, pids) in [
        (format!("{MAGIC}\n7\nend\n"), Ok(vec![7])),
        (format!("{MAGIC}\n7\n8\nend\n"), Ok(vec![7, 8])),
        (format!("{MAGIC}\n"), Err(io::ErrorKind::InvalidData)),
        // Torn after a complete first pid: not a complete multi-writer record.
        (format!("{MAGIC}\n7\n"), Err(io::ErrorKind::InvalidData)),
        (format!("{MAGIC}\nend\n"), Err(io::ErrorKind::InvalidData)),
        (
            format!("{MAGIC}\n0\nend\n"),
            Err(io::ErrorKind::InvalidData),
        ),
        (
            format!("{MAGIC}\n-7\nend\n"),
            Err(io::ErrorKind::InvalidData),
        ),
        (
            format!("{MAGIC}\nseven\nend\n"),
            Err(io::ErrorKind::InvalidData),
        ),
        (format!("{MAGIC}\n7\nend"), Err(io::ErrorKind::InvalidData)),
        (
            "cowshed-scratch-v2\n7\nend\n".to_owned(),
            Err(io::ErrorKind::InvalidData),
        ),
        ("partial".to_owned(), Err(io::ErrorKind::InvalidData)),
        (oversized, Err(io::ErrorKind::InvalidData)),
    ] {
        assert_eq!(
            owner_record(&text).map_err(|error| error.kind()),
            pids,
            "{text:?}"
        );
    }
}

/// One pass's turns over `scripts`, each root's names (`None`: it cannot be listed), executed as
/// the shell executes them, with each root's open listing kept in `listings` between passes.
/// Answers the turns and the entries the pass met.
fn drive<'a>(
    cursors: &mut Cursors,
    budget: usize,
    scripts: &[Option<&'a [&'a str]>],
    listings: &mut [Option<std::slice::Iter<'a, &'a str>>],
) -> (Vec<Turn>, Vec<&'a str>) {
    let mut pass = cursors.pass(budget);
    let (mut turns, mut met) = (Vec::new(), Vec::new());
    loop {
        let turn = cursors.turn(&mut pass);
        turns.push(turn);
        match turn {
            Turn::List(index) => {
                listings[index] = scripts[index].map(|names| names.iter());
                cursors.listed(&mut pass, index, listings[index].is_some());
            }
            Turn::Read(index) => {
                let entry = listings[index].as_mut().and_then(Iterator::next);
                if entry.is_none() {
                    listings[index] = None;
                }
                met.extend(entry.copied());
                cursors.read(&mut pass, index, entry.is_some());
            }
            Turn::End(_) => return (turns, met),
        }
    }
}

/// A pass meets at most its budget of entries across its roots, round robin. A root's listing
/// carries over to the next pass, so a pass never restarts behind entries one already met, and a
/// listing that ended is opened afresh; a root that cannot be listed costs the budget nothing, an
/// unreadable entry costs one.
#[test]
fn every_pass_bounds_its_entries_and_carries_each_roots_listing_over() {
    let scripts: [Option<&[&str]>; 3] = [
        Some(&["a0", "a1", "a2", "unreadable", "a4"]),
        Some(&["b0", "b1"]),
        None,
    ];
    let mut listings = [None, None, None];
    let mut cursors = Cursors::new(scripts.len());
    let mut pass = || drive(&mut cursors, 3, &scripts, &mut listings);
    use Turn::{End, List};
    assert_eq!(
        pass(),
        (
            vec![
                List(0),
                Turn::Read(0),
                List(1),
                Turn::Read(1),
                List(2),
                Turn::Read(0),
                End(3)
            ],
            vec!["a0", "b0", "a1"]
        )
    );
    assert_eq!(
        pass(),
        (
            vec![
                Turn::Read(1),
                List(2),
                Turn::Read(0),
                Turn::Read(1),
                Turn::Read(0),
                End(3)
            ],
            vec!["b1", "a2", "unreadable"]
        )
    );
    assert_eq!(
        pass(),
        (
            vec![
                List(1),
                Turn::Read(1),
                List(2),
                Turn::Read(0),
                Turn::Read(1),
                End(3)
            ],
            vec!["b0", "a4", "b1"]
        )
    );
    assert_eq!(
        pass(),
        (
            vec![List(2), Turn::Read(0), Turn::Read(1), End(0)],
            Vec::new()
        )
    );
    assert_eq!(
        pass(),
        (
            vec![
                List(2),
                List(0),
                Turn::Read(0),
                List(1),
                Turn::Read(1),
                Turn::Read(0),
                End(3)
            ],
            vec!["a0", "b0", "a1"]
        )
    );
}

/// Only socket scratch is deleted; anything else is archived whole, and a dry run says which.
#[test]
fn only_socket_scratch_is_deleted_and_a_dry_run_only_says_so() {
    assert_eq!(plan(Some(()), false), Plan::Delete(()));
    assert_eq!(plan(None::<()>, false), Plan::Archive);
    assert_eq!(plan(Some(()), true), Plan::WouldDelete);
    assert_eq!(plan(None::<()>, true), Plan::WouldArchive);
}

#[test]
fn run_output_moves_intact_to_trash_and_is_not_deleted_by_gc_or_the_first_background_pass() {
    let fixture = Fixture::new();
    let path = fixture.scratch("cowshed-sock-output", dead_pid());
    fs::write(path.join("trace.arrow"), b"only copy of a failing run").unwrap();
    let inode = fs::metadata(&path).unwrap().ino();
    let report = fixture.reaper().sweep(&fixture.store(), false);
    let archived = report
        .findings
        .iter()
        .find_map(|finding| match &finding.disposition {
            Disposition::Archived(path) => Some(path),
            _ => None,
        })
        .expect("output was archived, not deleted");
    assert!(!path.exists());
    assert!(archived.starts_with(fixture.store().join(".trash/scratch")));
    assert_eq!(
        fs::metadata(archived).unwrap().ino(),
        inode,
        "one rename, no copy/delete"
    );
    assert_eq!(
        fs::read(archived.join("trace.arrow")).unwrap(),
        b"only copy of a failing run"
    );
    let mut trash = TrashReaper::default();
    assert_eq!(trash.sweep(&fixture.store(), SystemTime::now()).unwrap(), 0);
    assert!(archived.join("trace.arrow").exists());
    assert!(
        trash
            .sweep(
                &fixture.store(),
                SystemTime::now() + EVIDENCE_RETENTION + Duration::from_secs(1)
            )
            .unwrap()
            > 0
    );
    assert!(!archived.exists());
}

/// The sweep's real pass over owned and planted entries: a dead owner's socket scratch is deleted,
/// and no link is followed, neither one standing for an entry nor one standing for an owner
/// record, though each leads to a dead owner's scratch or record.
#[test]
fn a_dead_owners_socket_scratch_is_deleted_and_no_link_is_followed() {
    let fixture = Fixture::new();
    let dead = dead_pid();
    let deleted = fixture.scratch("cowshed-sock-dead", dead);
    // A dead owner's scratch the sweep reaches only through a link: below a directory no record
    // owns, which the sweep passes over.
    fs::DirBuilder::new()
        .mode(0o700)
        .create(fixture.0.join("nest"))
        .unwrap();
    let target = fixture.scratch("nest/cowshed-sock-target", dead);
    let link = fixture.0.join("cowshed-sock-link");
    symlink(&target, &link).unwrap();
    // A private record naming the dead owner, outside the scratch whose record it would be.
    let record = fixture.0.join("foreign-record");
    fs::write(&record, format!("{MAGIC}\n{dead}\nend\n")).unwrap();
    fs::set_permissions(&record, fs::Permissions::from_mode(0o600)).unwrap();
    let linked = fixture.0.join("cowshed-sock-linked-record");
    fs::DirBuilder::new().mode(0o700).create(&linked).unwrap();
    fs::write(linked.join("s.sock.lock"), b"").unwrap();
    symlink(&record, linked.join(OWNER_FILE)).unwrap();
    let report = fixture.reaper().sweep(&fixture.store(), false);
    assert!(!deleted.exists());
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.path == deleted && finding.disposition == Disposition::Deleted),
        "{:?}",
        report.findings
    );
    assert!(link.is_symlink());
    assert!(
        target.join("s.sock.lock").exists(),
        "the link's target is untouched"
    );
    assert!(
        linked.join("s.sock.lock").exists(),
        "a linked record names no owner"
    );
}

#[test]
fn background_deletion_is_bounded_and_does_not_follow_links() {
    let fixture = Fixture::new();
    let path = fixture.scratch("cowshed-sock-large-output", dead_pid());
    for index in 0..PASS_ENTRIES * 2 {
        fs::write(path.join(index.to_string()), b"output").unwrap();
    }
    let outside = fixture.0.join("foreign-output");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("keep"), b"foreign").unwrap();
    symlink(&outside, path.join("outside")).unwrap();
    let report = fixture.reaper().sweep(&fixture.store(), false);
    let archived = report
        .findings
        .iter()
        .find_map(|finding| match &finding.disposition {
            Disposition::Archived(path) => Some(path),
            _ => None,
        })
        .unwrap();
    let mut trash = TrashReaper::default();
    let after = SystemTime::now() + EVIDENCE_RETENTION + Duration::from_secs(1);
    assert!(trash.sweep(&fixture.store(), after).unwrap() <= PASS_ENTRIES);
    assert!(archived.exists(), "no synchronous recursive delete");
    assert!(
        archived.join(OWNER_FILE).exists(),
        "deletion retains its ownership proof"
    );
    trash = TrashReaper::default();
    for _ in 0..3 {
        trash.sweep(&fixture.store(), after).unwrap();
    }
    assert!(!archived.exists());
    assert_eq!(fs::read(outside.join("keep")).unwrap(), b"foreign");
}

#[cfg(target_os = "linux")]
#[test]
fn mount_identity_uses_the_directory_mount_and_refuses_symlinks() {
    use std::os::fd::AsRawFd as _;

    let fixture = Fixture::new();
    let directory = fixture.0.join("directory");
    fs::create_dir(&directory).unwrap();
    let held = File::open(&directory).unwrap();
    let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{}", held.as_raw_fd())).unwrap();
    let expected = fdinfo
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .expect("the held directory has a kernel mount ID")
        .trim()
        .parse::<u64>()
        .unwrap();
    let metadata = fs::symlink_metadata(&directory).unwrap();
    assert_eq!(mount_identity(&directory, &metadata).unwrap(), expected);

    let redirected = fixture.0.join("redirected");
    symlink(&directory, &redirected).unwrap();
    assert!(mount_identity(&redirected, &fs::symlink_metadata(&redirected).unwrap()).is_err());
}

#[test]
fn a_surviving_socket_holder_keeps_a_dead_creators_directory() {
    let fixture = Fixture::new();
    let path = fixture.scratch("cowshed-sock-held", dead_pid());
    let lock = crate::fork_lock::Fenced::new(File::open(path.join("s.sock.lock")).unwrap());
    lock.lock_bounded().expect("hold socket lease");
    let report = fixture.reaper().sweep(&fixture.store(), false);
    assert!(path.exists());
    assert!(report.findings.iter().any(|finding| matches!(&finding.disposition, Disposition::Kept(reason) if reason.contains("socket lease is held"))));
    drop(lock);
    fixture.reaper().sweep(&fixture.store(), false);
    assert!(!path.exists());
}

#[test]
fn a_complete_first_pid_is_not_a_complete_multiwriter_record() {
    let fixture = Fixture::new();
    let dead = dead_pid();
    let path = fixture.scratch("cowshed-disk-image-torn", dead);
    fs::write(path.join(OWNER_FILE), format!("{MAGIC}\n{dead}\n")).unwrap();
    fixture.reaper().sweep(&fixture.store(), false);
    assert!(
        path.exists(),
        "missing end means publication was interrupted"
    );
}

#[test]
fn archive_names_without_ownership_and_redirected_trash_are_kept() {
    let fixture = Fixture::new();
    let archive = fixture
        .store()
        .join(".trash/scratch")
        .join(format!("1-{}", uuid::Uuid::new_v4().simple()));
    fs::create_dir_all(&archive).unwrap();
    fs::write(archive.join("trace.arrow"), b"not ours").unwrap();
    assert_eq!(
        TrashReaper::default()
            .sweep(&fixture.store(), SystemTime::now())
            .unwrap(),
        0
    );
    assert_eq!(fs::read(archive.join("trace.arrow")).unwrap(), b"not ours");

    let other_store = fixture.0.join("redirected");
    fs::create_dir_all(other_store.join(".trash")).unwrap();
    symlink(
        fixture.store().join(".trash/scratch"),
        other_store.join(".trash/scratch"),
    )
    .unwrap();
    assert!(
        TrashReaper::default()
            .sweep(&other_store, SystemTime::now())
            .is_err()
    );
    assert!(archive.join("trace.arrow").exists());
}

#[test]
fn image_fixture_scratch_stays_with_its_detach_aware_owner() {
    let fixture = Fixture::new();
    let path = fixture.scratch("cowshed-itest-dead", dead_pid());
    let report = fixture.reaper().sweep(&fixture.store(), false);
    assert!(path.exists());
    assert!(report.findings.iter().any(|finding| matches!(&finding.disposition, Disposition::Kept(reason) if reason.contains("detach-aware"))));
}

#[test]
fn a_named_owner_is_accepted_only_for_its_exact_fixture_family() {
    for (name, family, owner) in [
        (
            "cowshed-signing-p0000007b0000001",
            "cowshed-signing",
            Some(123),
        ),
        ("cs-signing-p0000007b0000001", "cs-signing", Some(123)),
        (
            "cowshed-signing-extra-p0000007b0000001",
            "cowshed-signing",
            None,
        ),
        ("cowshed-signing-p0000007b0000001", "cowshed-other", None),
        ("cowshed-signing-punfinished", "cowshed-signing", None),
        ("cowshed-signing-p000000000000001", "cowshed-signing", None),
        ("cowshed-signing-p800000000000001", "cowshed-signing", None),
        ("cowshed-signing-p0000007B0000001", "cowshed-signing", None),
        ("foreign-p0000007b0000001", "foreign", None),
        ("cowshed-signing-0123456789abcdef", "cowshed-signing", None),
    ] {
        assert_eq!(
            named_owner_for(Path::new(name), family),
            owner,
            "{name} for {family}"
        );
    }
}

#[test]
fn named_ownership_preserves_the_fixture_socket_path_budget() {
    for prefix in ["cowshed-recovering", "cs-pr"] {
        let path = named_path(Path::new("/tmp"), prefix, i32::MAX.unsigned_abs());
        assert_eq!(named_owner(&path), Some(i32::MAX));
        // Exactly main's TempRoot name length, the one its socket fixtures were sized for.
        assert_eq!(path.file_name().unwrap().len(), prefix.len() + NAMED_SUFFIX);
    }
    // Another tool's prefix is not claimed, and an unowned 16-hex fixture name names no owner.
    assert_eq!(
        named_owner(&named_path(Path::new("/tmp"), "fixture", 1)),
        None
    );
    assert_eq!(
        named_owner(Path::new("/tmp/cowshed-sock-0123456789abcdef")),
        None
    );
    // The longest socket path below a named fixture: the longest scratch parent, a workspace's
    // runtime link /tmp/cs-<12 hex> (cowshed-core sandbox.rs workspace_runtime_link; 08_testing.md
    // "Fixtures"), the longest prefix of a store holding a supervisor socket (supervisor_manager.rs
    // recovery_store), the largest creator pid, and the supervisor's unpublished listener
    // run/<32 hex>.bind (supervisor_socket.rs bind). Building the production sockaddr needs no
    // write grant to the host's shared temp root.
    let longest = named_path(
        Path::new("/tmp/cs-0123456789ab"),
        "cowshed-recovering",
        i32::MAX.unsigned_abs(),
    );
    let listener = |fixture: &Path| fixture.join("run").join(format!("{:032x}.bind", u128::MAX));
    std::os::unix::net::SocketAddr::from_pathname(listener(&longest)).unwrap();
    // The control: the branch's earlier prefix + 33 byte name does not fit there.
    let mut wider = longest.into_os_string();
    wider.push("0123456789abcdef");
    assert!(std::os::unix::net::SocketAddr::from_pathname(listener(Path::new(&wider))).is_err());
}

#[test]
fn named_fixture_ownership_leaves_payload_empty_and_reaps_only_dead_creators() {
    let fixture = Fixture::new();
    let live = create_named(&fixture.0, "cowshed-sock").unwrap();
    assert_eq!(
        fs::read_dir(&live).unwrap().count(),
        0,
        "no ownership file in a Git or telemetry root"
    );
    assert_eq!(
        owners(&live).unwrap(),
        [i32::try_from(std::process::id()).unwrap()]
    );
    File::open(&live)
        .unwrap()
        .set_times(FileTimes::new().set_modified(UNIX_EPOCH))
        .unwrap();
    let dead = named_path(&fixture.0, "cowshed-mgr", dead_pid());
    fs::DirBuilder::new().mode(0o700).create(&dead).unwrap();
    fs::write(dead.join("s.sock.lock"), b"").unwrap();
    fixture.reaper().sweep(&fixture.store(), false);
    assert!(live.exists());
    assert!(!dead.exists());
}

#[test]
fn archiving_named_fixture_output_carries_ownership_past_the_rename() {
    let fixture = Fixture::new();
    let pid = dead_pid();
    let path = named_path(&fixture.0, "cs-trace", pid);
    fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
    fs::write(path.join("trace.arrow"), b"only evidence").unwrap();
    fixture.reaper().sweep(&fixture.store(), true);
    assert!(!path.join(OWNER_FILE).exists(), "dry-run adds no record");
    let report = fixture.reaper().sweep(&fixture.store(), false);
    let archive = report
        .findings
        .iter()
        .find_map(|finding| match &finding.disposition {
            Disposition::Archived(path) => Some(path),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        fs::read(archive.join("trace.arrow")).unwrap(),
        b"only evidence"
    );
    assert_eq!(owners(archive).unwrap(), [i32::try_from(pid).unwrap()]);
    assert!(
        TrashReaper::default()
            .sweep(
                &fixture.store(),
                SystemTime::now() + EVIDENCE_RETENTION + Duration::from_secs(1)
            )
            .unwrap()
            > 0
    );
    assert!(!archive.exists());
}

#[test]
fn a_named_creator_does_not_override_a_malformed_multiwriter_record() {
    let fixture = Fixture::new();
    let path = named_path(&fixture.0, "cowshed-sock", dead_pid());
    fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
    fs::write(path.join(OWNER_FILE), b"partial").unwrap();
    fixture.reaper().sweep(&fixture.store(), false);
    assert!(path.exists());
}

#[test]
fn an_archive_the_walk_cannot_finish_stays_owned_and_never_starves_the_archives_behind_it() {
    let fixture = Fixture::new();
    let creator = dead_pid();
    let stuck = fixture.scratch("cowshed-sock-stuck", creator);
    fs::create_dir(stuck.join("held")).unwrap();
    fs::write(stuck.join("held/trace.arrow"), b"evidence").unwrap();
    let behind = fixture.scratch("cowshed-sock-behind", dead_pid());
    fs::write(behind.join("trace.arrow"), b"evidence").unwrap();
    let report = fixture.reaper().sweep(&fixture.store(), false);
    let archive_of = |source: &Path| {
        report
            .findings
            .iter()
            .find_map(|finding| match &finding.disposition {
                Disposition::Archived(to) if finding.path == source => Some(to.clone()),
                _ => None,
            })
            .expect("dead output is archived")
    };
    let (stuck, behind) = (archive_of(&stuck), archive_of(&behind));
    // A real subtree this user cannot read: entering it fails.
    fs::set_permissions(stuck.join("held"), fs::Permissions::from_mode(0o000)).unwrap();
    let after = SystemTime::now() + EVIDENCE_RETENTION + Duration::from_secs(1);
    let mut trash = TrashReaper::default();
    let refusals = (0..3)
        .filter_map(|_| trash.sweep(&fixture.store(), after).err())
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    let readable = fs::set_permissions(stuck.join("held"), fs::Permissions::from_mode(0o700));
    assert!(
        !behind.exists(),
        "the archive behind it is still deleted: {refusals:?}"
    );
    assert_eq!(
        owners(&stuck).unwrap(),
        [i32::try_from(creator).unwrap()],
        "an unfinished archive keeps its ownership record"
    );
    assert!(
        refusals
            .iter()
            .any(|refusal| refusal.contains(&format!("kept scratch archive {}", stuck.display()))),
        "{refusals:?}"
    );
    readable.unwrap();
    for _ in 0..2 {
        trash.sweep(&fixture.store(), after).unwrap();
    }
    assert!(
        !stuck.exists(),
        "the next listing walks it again from its root"
    );
}

#[test]
fn an_archive_root_that_outlives_its_record_gets_the_record_back() {
    let fixture = Fixture::new();
    let creator = dead_pid();
    let source = fixture.scratch("cowshed-sock-pinned", creator);
    fs::write(source.join("trace.arrow"), b"evidence").unwrap();
    let report = fixture.reaper().sweep(&fixture.store(), false);
    let archive = report
        .findings
        .iter()
        .find_map(|finding| match &finding.disposition {
            Disposition::Archived(to) => Some(to.clone()),
            _ => None,
        })
        .expect("dead output is archived");
    let record = fs::read(archive.join(OWNER_FILE)).unwrap();
    // The archive empties, but its root cannot leave a trash directory this user cannot write.
    let trash_dir = fixture.store().join(".trash/scratch");
    fs::set_permissions(&trash_dir, fs::Permissions::from_mode(0o500)).unwrap();
    let after = SystemTime::now() + EVIDENCE_RETENTION + Duration::from_secs(1);
    let mut trash = TrashReaper::default();
    let refused = trash.sweep(&fixture.store(), after);
    let writable = fs::set_permissions(&trash_dir, fs::Permissions::from_mode(0o700));
    let refused = refused.expect_err("the root outlives its removal");
    assert!(
        refused
            .to_string()
            .contains(&format!("kept scratch archive {}", archive.display())),
        "{refused}"
    );
    assert_eq!(
        fs::read(archive.join(OWNER_FILE)).unwrap(),
        record,
        "the exact record is back"
    );
    assert_eq!(owners(&archive).unwrap(), [i32::try_from(creator).unwrap()]);
    writable.unwrap();
    for _ in 0..2 {
        trash.sweep(&fixture.store(), after).unwrap();
    }
    assert!(
        !archive.exists(),
        "the restored record lets the next listing finish it"
    );
}

#[test]
fn host_roots_never_enter_a_bounded_exec_ram_lease() {
    for temp in [
        // The TMPDIR bounded-exec gives its command: a lease below the volume
        // mounted at the per-user root's ram<revision> leaf, at any depth below it.
        "/private/var/folders/cowshed/4242/ram3/12345-aBcDeF",
        "/private/var/folders/cowshed/4242/ram3/12345-aBcDeF/nested",
        "/private/var/folders/cowshed/4242/ram3",
        "/private/var/folders/cowshed/4242/ram3/.archive/1700000000-12345-aBcDeF",
        "/private/var/folders/cowshed/4242/ram3-new",
        // Another revision's checkout mounts beside it, never on one directory.
        "/private/var/folders/cowshed/4242/ram4/12345-aBcDeF",
        // A test namespace's directory in the per-user root holds its own RAM volume at the same
        // leaf, at any depth below it.
        "/private/var/folders/cowshed/4242/0214h03/ram3",
        "/private/var/folders/cowshed/4242/0214h03/ram3-new",
        "/private/var/folders/cowshed/4242/0214h03/ram3/12345-aBcDeF",
        // Revisions 1-2 mounted by volume name.
        "/Volumes/smoo-ram2-4242/12345-aBcDeF",
        "/Volumes/smoo-ram-4242/12345-aBcDeF",
    ] {
        assert_eq!(
            host_roots(PathBuf::from(temp)),
            vec![PathBuf::from("/tmp")],
            "RAM-owned temp root {temp} must be left to bounded-exec"
        );
    }
}

#[test]
fn host_roots_still_sweep_non_ram_temp_roots_with_ram_shaped_names() {
    for temp in [
        "/private/var/folders/cowshed/4242/t",
        "/private/var/folders/cowshed/4242/t/ram3",
        "/private/var/folders/cowshed/4242/mnt",
        "/private/var/folders/cowshed/4242/ram3x/lease",
        "/private/var/folders/cowshed/4242/ram/lease",
        "/private/var/folders/cowshed/4242/ram-new/lease",
        "/private/var/folders/cowshed/4242/rambunctious/lease",
        "/private/var/folders/cowshed/not-a-uid/ram3/lease",
        "/private/var/folders/other/4242/ram3/lease",
        "/private/tmp/ram3/lease",
        "/tmp/smoo-ram-notes/temp",
        "/Volumes/other-volume/ram3/lease",
        "/private/var/folders/ab/cdef/T",
        // A namespace's directory holds a RAM volume only at its ram<revision> leaf, and a name
        // that is not a namespace's (it starts with a letter) holds none.
        "/private/var/folders/cowshed/4242/0214h03/t",
        "/private/var/folders/cowshed/4242/abcdefg/ram3",
        // The shape the fixtures' RAM volumes had, mounted by name below t, is no RAM slot.
        "/private/var/folders/cowshed/4242/t/smoo-ram-test-31736-0/12345-aBcDeF",
        "/private/var/folders/cowshed/4242/t/smoo-ram-test-31736-0-new",
    ] {
        assert_eq!(
            host_roots(PathBuf::from(temp)),
            vec![PathBuf::from("/tmp"), PathBuf::from(temp)],
            "non-RAM temp root {temp} stays in the sweep"
        );
    }
}

#[test]
fn a_reaper_built_for_a_ram_lease_tmpdir_holds_no_ram_root() {
    let reaper = Reaper::new(host_roots(PathBuf::from(
        "/private/var/folders/cowshed/4242/ram3/12345-aBcDeF",
    )))
    .unwrap();
    assert_eq!(reaper.roots.len(), 1);
    assert_eq!(
        reaper.roots[0].path,
        PathBuf::from("/tmp").canonicalize().unwrap()
    );
}
