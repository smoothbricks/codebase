use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use cowshed_core::api::{
    AdmissionCommitment, CONTROLLER_COMMITMENT_VERSION, ControllerCommitment, JobId, JobState,
    Sha256Digest, TerminalCommitment, WorkspaceIncarnation,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::commitment_store::{
    CommitmentDate, CommitmentPublicationPoint, CommitmentStore, CommitmentStoreEnvironment,
    CommitmentStoreError,
};
use cowshed_core::storage::job_artifact::{
    controller_commitment_schema, controller_commitments_to_batch,
};
use uuid::Uuid;

const INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c80";
const OTHER_INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c81";
static SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "cowshed-commitment-store-{label}-{}-{nonce}",
            std::process::id()
        ));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct FixedEnvironment {
    date: CommitmentDate,
    fail_at: Option<CommitmentPublicationPoint>,
    count_syncs: bool,
}

impl FixedEnvironment {
    fn new(date: CommitmentDate) -> Self {
        Self {
            date,
            fail_at: None,
            count_syncs: false,
        }
    }

    fn failing(date: CommitmentDate, point: CommitmentPublicationPoint) -> Self {
        Self {
            date,
            fail_at: Some(point),
            count_syncs: false,
        }
    }

    fn counting(date: CommitmentDate) -> Self {
        Self {
            date,
            fail_at: None,
            count_syncs: true,
        }
    }
}

impl CommitmentStoreEnvironment for FixedEnvironment {
    fn utc_date(&self) -> io::Result<CommitmentDate> {
        Ok(self.date)
    }

    fn sync_directory(&self, directory: &File) -> io::Result<()> {
        directory.sync_all()?;
        if self.count_syncs {
            SYNC_CALLS.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    fn publication_point(&self, point: CommitmentPublicationPoint) -> io::Result<()> {
        if self.fail_at == Some(point) {
            Err(io::Error::other("injected publication crash"))
        } else {
            Ok(())
        }
    }
}

struct DateSequenceEnvironment {
    dates: [CommitmentDate; 2],
    next: AtomicUsize,
}

impl CommitmentStoreEnvironment for DateSequenceEnvironment {
    fn utc_date(&self) -> io::Result<CommitmentDate> {
        let index = self.next.fetch_add(1, Ordering::SeqCst).min(1);
        Ok(self.dates[index])
    }
}

fn repo() -> RepoId {
    RepoId::parse("acme/widget").unwrap()
}

fn incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(INCARNATION).unwrap()
}

fn other_incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(OTHER_INCARNATION).unwrap()
}

fn date(year: u16, month: u8, day: u8) -> CommitmentDate {
    CommitmentDate::new(year, month, day).unwrap()
}

fn store_with_environment(
    root: &Path,
    environment: impl CommitmentStoreEnvironment + 'static,
) -> Result<CommitmentStore, CommitmentStoreError> {
    CommitmentStore::open_with_environment(root, repo(), [incarnation()], Box::new(environment))
}

fn admission(order: u64, job_id: u64) -> ControllerCommitment {
    ControllerCommitment::Admission(AdmissionCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order,
        repo_id: repo(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(job_id).unwrap(),
        grant_revision: 7,
    })
}

fn terminal(order: u64, job_id: u64) -> ControllerCommitment {
    let empty = Sha256Digest::compute(&[]);
    ControllerCommitment::Terminal(TerminalCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order,
        repo_id: repo(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(job_id).unwrap(),
        state: JobState::Exited,
        grant_revision: 7,
        stdout_bytes: 0,
        stdout_sha256: empty,
        stderr_bytes: 0,
        stderr_sha256: empty,
        batch_sha256: Sha256Digest::compute(b"terminal batch"),
        output_limit: None,
    })
}

fn sealed_name(order: u64, writer: Uuid) -> String {
    format!("commitment-{order:020}-{}.arrow", writer.hyphenated())
}

fn sealed_segments(root: &Path) -> Vec<PathBuf> {
    let mut segments = Vec::new();
    if !root.exists() {
        return segments;
    }
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_dir() {
            continue;
        }
        for child in fs::read_dir(entry.path()).unwrap() {
            let child = child.unwrap();
            let name = child.file_name();
            if name.as_encoded_bytes().starts_with(b"commitment-")
                && name.as_encoded_bytes().ends_with(b".arrow")
            {
                segments.push(child.path());
            }
        }
    }
    segments.sort();
    segments
}

fn write_batches(path: &Path, batches: &[ControllerCommitment]) {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut file = file;
    let first = controller_commitments_to_batch(std::slice::from_ref(&batches[0])).unwrap();
    let mut writer = StreamWriter::try_new(&mut file, &first.schema()).unwrap();
    for commitment in batches {
        let batch = controller_commitments_to_batch(std::slice::from_ref(commitment)).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    file.flush().unwrap();
    file.sync_all().unwrap();
}

fn write_single_segment(
    root: &Path,
    partition: CommitmentDate,
    order: u64,
    commitment: &ControllerCommitment,
) -> PathBuf {
    let directory = root.join(partition.to_string());
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join(sealed_name(order, Uuid::new_v4()));
    write_batches(&path, std::slice::from_ref(commitment));
    path
}

fn assert_open_integrity(root: &Path) {
    assert!(matches!(
        CommitmentStore::open(root, repo(), [incarnation()]),
        Err(CommitmentStoreError::Integrity { .. })
    ));
}

#[test]
fn publish_reopen_round_trip_uses_exact_schema_and_one_segment_per_commitment() {
    let root = TempRoot::new("round-trip");
    let partition = date(2026, 2, 3);
    let mut store = store_with_environment(root.path(), FixedEnvironment::new(partition)).unwrap();

    assert_eq!(store.next_order().unwrap(), 1);
    store.publish(admission(1, 1)).unwrap();
    store.publish(terminal(2, 1)).unwrap();
    assert_eq!(store.next_order().unwrap(), 3);

    let segments = sealed_segments(root.path());
    assert_eq!(segments.len(), 2);
    for segment in &segments {
        let file = File::open(segment).unwrap();
        let mut reader = StreamReader::try_new(file, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema(), controller_commitment_schema());
        assert_eq!(batch.num_rows(), 1);
        assert!(reader.next().is_none());
    }

    drop(store);
    let reopened = CommitmentStore::open(root.path(), repo(), [incarnation()]).unwrap();
    assert_eq!(reopened.next_order().unwrap(), 3);
}

#[test]
fn order_remains_contiguous_across_utc_date_partitions() {
    let root = TempRoot::new("cross-date");
    let environment = DateSequenceEnvironment {
        dates: [date(2026, 12, 31), date(2027, 1, 1)],
        next: AtomicUsize::new(0),
    };
    let mut store = store_with_environment(root.path(), environment).unwrap();
    store.publish(admission(1, 1)).unwrap();
    store.publish(terminal(2, 1)).unwrap();

    let segments = sealed_segments(root.path());
    assert_eq!(segments.len(), 2);
    let partitions: Vec<_> = segments
        .iter()
        .map(|segment| segment.parent().unwrap().file_name().unwrap())
        .collect();
    assert!(partitions.contains(&std::ffi::OsStr::new("2026-12-31")));
    assert!(partitions.contains(&std::ffi::OsStr::new("2027-01-01")));
    drop(store);
    assert_eq!(
        CommitmentStore::open(root.path(), repo(), [incarnation()])
            .unwrap()
            .next_order()
            .unwrap(),
        3
    );
}

#[test]
fn publish_validation_rejects_foreign_unknown_and_invalid_job_transitions() {
    let root = TempRoot::new("validation");
    let mut store =
        store_with_environment(root.path(), FixedEnvironment::new(date(2026, 4, 5))).unwrap();

    let mut foreign = admission(1, 1);
    let ControllerCommitment::Admission(value) = &mut foreign else {
        unreachable!()
    };
    value.repo_id = RepoId::parse("other/repository").unwrap();
    assert!(matches!(
        store.publish(foreign),
        Err(CommitmentStoreError::Integrity { .. })
    ));

    let mut unknown = admission(1, 1);
    let ControllerCommitment::Admission(value) = &mut unknown else {
        unreachable!()
    };
    value.workspace_incarnation = other_incarnation();
    assert!(matches!(
        store.publish(unknown),
        Err(CommitmentStoreError::Integrity { .. })
    ));
    assert!(matches!(
        store.publish(terminal(1, 1)),
        Err(CommitmentStoreError::Integrity { .. })
    ));
    assert_eq!(store.next_order().unwrap(), 1);
    assert!(sealed_segments(root.path()).is_empty());

    store.publish(admission(1, 1)).unwrap();
    store.publish(terminal(2, 1)).unwrap();
    assert!(matches!(
        store.publish(terminal(3, 1)),
        Err(CommitmentStoreError::Integrity { .. })
    ));
    assert_eq!(store.next_order().unwrap(), 3);
    assert_eq!(sealed_segments(root.path()).len(), 2);
}

#[test]
fn recovery_revalidates_foreign_unknown_and_terminal_history() {
    let partition = date(2026, 5, 6);

    let foreign_root = TempRoot::new("recover-foreign");
    let mut foreign = admission(1, 1);
    let ControllerCommitment::Admission(value) = &mut foreign else {
        unreachable!()
    };
    value.repo_id = RepoId::parse("other/repository").unwrap();
    write_single_segment(foreign_root.path(), partition, 1, &foreign);
    assert_open_integrity(foreign_root.path());

    let unknown_root = TempRoot::new("recover-unknown");
    let mut unknown = admission(1, 1);
    let ControllerCommitment::Admission(value) = &mut unknown else {
        unreachable!()
    };
    value.workspace_incarnation = other_incarnation();
    write_single_segment(unknown_root.path(), partition, 1, &unknown);
    assert_open_integrity(unknown_root.path());

    let terminal_root = TempRoot::new("recover-terminal-first");
    write_single_segment(terminal_root.path(), partition, 1, &terminal(1, 1));
    assert_open_integrity(terminal_root.path());

    let duplicate_root = TempRoot::new("recover-terminal-duplicate");
    write_single_segment(duplicate_root.path(), partition, 1, &admission(1, 1));
    write_single_segment(duplicate_root.path(), partition, 2, &terminal(2, 1));
    write_single_segment(duplicate_root.path(), partition, 3, &terminal(3, 1));
    assert_open_integrity(duplicate_root.path());
}

#[test]
fn recovery_rejects_symlink_hardlink_and_malformed_segment_names() {
    let partition = date(2026, 6, 7);

    let symlink_root = TempRoot::new("symlink");
    let segment = write_single_segment(symlink_root.path(), partition, 1, &admission(1, 1));
    let target = symlink_root.path().join("moved-segment");
    fs::rename(&segment, &target).unwrap();
    symlink(&target, &segment).unwrap();
    assert_open_integrity(symlink_root.path());

    let hardlink_root = TempRoot::new("hardlink");
    let segment = write_single_segment(hardlink_root.path(), partition, 1, &admission(1, 1));
    fs::hard_link(&segment, hardlink_root.path().join("alias")).unwrap();
    assert_open_integrity(hardlink_root.path());

    let malformed_root = TempRoot::new("malformed");
    let directory = malformed_root.path().join(partition.to_string());
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("commitment-not-an-order.arrow"),
        b"not Arrow",
    )
    .unwrap();
    assert_open_integrity(malformed_root.path());
}

#[test]
fn recovery_rejects_multiple_batches_order_gaps_and_duplicate_orders() {
    let partition = date(2026, 7, 8);

    let batches_root = TempRoot::new("multiple-batches");
    let path = batches_root
        .path()
        .join(partition.to_string())
        .join(sealed_name(1, Uuid::new_v4()));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    write_batches(&path, &[admission(1, 1), admission(1, 1)]);
    assert_open_integrity(batches_root.path());

    let gap_root = TempRoot::new("order-gap");
    write_single_segment(gap_root.path(), partition, 2, &admission(2, 1));
    assert_open_integrity(gap_root.path());

    let duplicate_root = TempRoot::new("duplicate-order");
    let original = write_single_segment(duplicate_root.path(), partition, 1, &admission(1, 1));
    let duplicate = original
        .parent()
        .unwrap()
        .join(sealed_name(1, Uuid::new_v4()));
    fs::copy(original, duplicate).unwrap();
    assert_open_integrity(duplicate_root.path());
}

#[test]
fn failure_before_rename_cleans_temporary_and_does_not_advance() {
    let root = TempRoot::new("before-rename");
    let partition = date(2026, 8, 9);
    let mut store = store_with_environment(
        root.path(),
        FixedEnvironment::failing(partition, CommitmentPublicationPoint::BeforeRename),
    )
    .unwrap();

    assert!(matches!(
        store.publish(admission(1, 1)),
        Err(CommitmentStoreError::Io { .. })
    ));
    assert_eq!(store.next_order().unwrap(), 1);
    assert!(sealed_segments(root.path()).is_empty());
    let entries: Vec<_> = fs::read_dir(root.path().join(partition.to_string()))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(entries.is_empty());
}

#[test]
fn failure_after_rename_is_recovered_without_in_memory_advance() {
    let root = TempRoot::new("after-rename");
    let partition = date(2026, 9, 10);
    let mut store = store_with_environment(
        root.path(),
        FixedEnvironment::failing(
            partition,
            CommitmentPublicationPoint::AfterRenameAndDirectorySync,
        ),
    )
    .unwrap();

    assert!(matches!(
        store.publish(admission(1, 1)),
        Err(CommitmentStoreError::Io { .. })
    ));
    assert_eq!(store.next_order().unwrap(), 1);
    assert_eq!(sealed_segments(root.path()).len(), 1);
    drop(store);

    let reopened = CommitmentStore::open(root.path(), repo(), [incarnation()]).unwrap();
    assert_eq!(reopened.next_order().unwrap(), 2);
}

#[test]
fn published_segment_is_private_and_parent_directory_is_synced() {
    SYNC_CALLS.store(0, Ordering::SeqCst);
    let root = TempRoot::new("mode-and-sync");
    let mut store =
        store_with_environment(root.path(), FixedEnvironment::counting(date(2026, 10, 11)))
            .unwrap();
    store.publish(admission(1, 1)).unwrap();

    let segments = sealed_segments(root.path());
    assert_eq!(segments.len(), 1);
    let metadata = fs::metadata(&segments[0]).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(metadata.nlink(), 1);
    assert_eq!(SYNC_CALLS.load(Ordering::SeqCst), 2);
}

#[test]
fn existing_sealed_order_conflicts_without_overwrite_or_state_advance() {
    let root = TempRoot::new("existing-destination");
    let partition = date(2026, 10, 12);
    let mut store = store_with_environment(root.path(), FixedEnvironment::new(partition)).unwrap();
    let directory = root.path().join(partition.to_string());
    fs::create_dir_all(&directory).unwrap();
    let segment = directory.join(sealed_name(1, store.writer_id()));
    write_batches(&segment, &[admission(1, 1)]);
    let original = fs::read(&segment).unwrap();

    assert!(matches!(
        store.publish(admission(1, 2)),
        Err(CommitmentStoreError::Conflict { order: 1 })
    ));
    assert_eq!(store.next_order().unwrap(), 1);
    assert_eq!(fs::read(&segment).unwrap(), original);
    assert_eq!(sealed_segments(root.path()), vec![segment]);
}

#[test]
fn concurrent_stores_cannot_publish_the_same_global_order() {
    let root = TempRoot::new("race");
    let partition = date(2026, 11, 12);
    let mut first = store_with_environment(root.path(), FixedEnvironment::new(partition)).unwrap();
    let mut second = store_with_environment(root.path(), FixedEnvironment::new(partition)).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);

    let first_thread = std::thread::spawn(move || {
        first_barrier.wait();
        first.publish(admission(1, 1))
    });
    let second_thread = std::thread::spawn(move || {
        second_barrier.wait();
        second.publish(admission(1, 2))
    });
    let results = [first_thread.join().unwrap(), second_thread.join().unwrap()];

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(CommitmentStoreError::Conflict { order: 1 })))
            .count(),
        1
    );
    let segments = sealed_segments(root.path());
    assert_eq!(segments.len(), 1);
    assert_eq!(
        CommitmentStore::open(root.path(), repo(), [incarnation()])
            .unwrap()
            .next_order()
            .unwrap(),
        2
    );
}
