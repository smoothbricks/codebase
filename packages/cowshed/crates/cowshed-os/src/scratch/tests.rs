use super::*;
use crate::fork_lock::Spawn as _;
use std::fs::{File, FileTimes};
use std::os::unix::fs::symlink;
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
    assert!(child.wait().unwrap().success());
    pid
}

#[test]
fn death_not_age_reclaims_a_fresh_owner_and_keeps_an_ancient_live_owner() {
    let fixture = Fixture::new();
    let dead = fixture.scratch("cowshed-sock-dead", dead_pid());
    let live = fixture.scratch("cowshed-mgr-live", std::process::id());
    File::open(&live)
        .unwrap()
        .set_times(FileTimes::new().set_modified(UNIX_EPOCH))
        .unwrap();
    let report = fixture.reaper().sweep(&fixture.store(), false);
    assert!(!dead.exists());
    assert!(live.join("s.sock.lock").exists());
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.path == dead && finding.disposition == Disposition::Deleted)
    );
    assert!(report.findings.iter().any(|finding| finding.path == live && matches!(&finding.disposition, Disposition::Kept(reason) if reason.contains("is live"))));
    // A fresh live directory survives too: there is no timestamp cutoff in either direction.
    File::open(&live)
        .unwrap()
        .set_times(FileTimes::new().set_modified(SystemTime::now()))
        .unwrap();
    fixture.reaper().sweep(&fixture.store(), false);
    assert!(live.exists());
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

#[test]
fn unknown_owners_foreign_names_and_symlinks_never_become_dead_owners() {
    let fixture = Fixture::new();
    let dead = dead_pid();
    let unknown = fixture.0.join("cowshed-sock-legacy");
    fs::DirBuilder::new().mode(0o700).create(&unknown).unwrap();
    fs::write(unknown.join("s.sock.lock"), b"").unwrap();
    for name in [
        "node_a",
        "bash_a",
        "cargo_a",
        "bun_a",
        "devenv-a",
        "sccache_a",
        "smoo-ram2-100",
    ] {
        fixture.scratch(name, dead);
    }
    let output = fixture.scratch("other-tool", std::process::id());
    let link = fixture.0.join("cowshed-sock-link");
    symlink(&output, &link).unwrap();
    let malformed = fixture.scratch("cowshed-sock-malformed", dead);
    fs::write(malformed.join(OWNER_FILE), b"cowshed-scratch-v1\n").unwrap();
    let linked = fixture.scratch("cowshed-sock-linked-record", dead);
    fs::remove_file(linked.join(OWNER_FILE)).unwrap();
    symlink(output.join(OWNER_FILE), linked.join(OWNER_FILE)).unwrap();
    let report = fixture.reaper().sweep(&fixture.store(), false);
    assert!(report.findings.iter().any(|finding| finding.path == unknown && matches!(&finding.disposition, Disposition::Kept(reason) if reason.contains("unknown owner"))));
    assert!(malformed.exists());
    assert!(linked.exists());
    assert!(link.is_symlink());
    assert!(output.exists());
    for name in [
        "node_a",
        "bash_a",
        "cargo_a",
        "bun_a",
        "devenv-a",
        "sccache_a",
        "smoo-ram2-100",
    ] {
        assert!(
            fixture.0.join(name).exists(),
            "never touch {name}, even with a marker"
        );
    }
}

#[test]
fn dry_run_is_read_only_and_all_recorded_writers_must_be_dead() {
    let fixture = Fixture::new();
    let dead = dead_pid();
    let path = fixture.scratch("cowshed-disk-image-output", dead);
    fs::write(path.join("stdout"), b"disk command output").unwrap();
    let report = fixture.reaper().sweep(&fixture.store(), true);
    assert_eq!(report.findings[0].disposition, Disposition::WouldArchive);
    assert!(path.exists());
    assert!(!fixture.store().exists());
    fs::remove_file(path.join(OWNER_FILE)).unwrap();
    record_owners(&path, &[dead, std::process::id()]).unwrap();
    fixture.reaper().sweep(&fixture.store(), false);
    assert!(
        path.exists(),
        "a detached writer still owns it after the parent died"
    );
}

#[test]
fn every_pass_bounds_enumeration_and_cursors_eventually_pass_live_entries() {
    let fixture = Fixture::new();
    let dead = dead_pid();
    let mut paths = Vec::new();
    for index in 0..(PASS_ENTRIES * 2 + 3) {
        paths.push(fixture.scratch(
            &format!("cowshed-sock-{index}"),
            if index % 2 == 0 {
                std::process::id()
            } else {
                dead
            },
        ));
    }
    let mut reaper = fixture.reaper();
    for _ in 0..4 {
        let report = reaper.sweep(&fixture.store(), false);
        assert!(report.examined <= PASS_ENTRIES);
    }
    for (index, path) in paths.iter().enumerate() {
        assert_eq!(
            path.exists(),
            index % 2 == 0,
            "live entries cannot starve dead entries"
        );
    }
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

#[test]
fn a_surviving_socket_holder_keeps_a_dead_creators_directory() {
    let fixture = Fixture::new();
    let path = fixture.scratch("cowshed-sock-held", dead_pid());
    let lock = crate::fork_lock::Fenced::new(File::open(path.join("s.sock.lock")).unwrap());
    lock.lock().expect("hold socket lease");
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
