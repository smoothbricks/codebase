//! The controller audit sink is telemetry: one sealed Arrow segment per record, never read back
//! for a decision. These tests pin the writer's durability discipline, the record contract the
//! segments carry, the `off` and injected-sink seams, and the publisher's health accounting.

use std::fs::{self, File};
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_ipc::reader::StreamReader;
use cowshed_core::api::{ControllerCommitment, JobId, WorkspaceIncarnation};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::supervisor::{CommitmentPublisher, CommitmentSink};
use cowshed_core::storage::audit::{
    ArrowAuditSink, AuditSink, AuditSinkEnvironment, AuditSinkError, CommitmentDate,
    CommitmentDraft, CommitmentPublicationPoint, ContinuityAudit, NullAuditSink,
};
use cowshed_core::storage::job_artifact::{
    controller_commitment_schema, decode_controller_commitments,
};

const INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c80";
static SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "cowshed-audit-sink-{label}-{}-{nonce}",
            std::process::id()
        )))
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

impl AuditSinkEnvironment for FixedEnvironment {
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

fn sink_with(root: &Path, environment: FixedEnvironment) -> ArrowAuditSink {
    ArrowAuditSink::open_with_environment(root, Box::new(environment)).unwrap()
}

fn date(year: u16, month: u8, day: u8) -> CommitmentDate {
    CommitmentDate::new(year, month, day).unwrap()
}

fn repo() -> RepoId {
    RepoId::parse("acme/widget").unwrap()
}

fn incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(INCARNATION).unwrap()
}

fn introduced() -> CommitmentDraft {
    CommitmentDraft::WorkspaceIntroduced {
        repo_id: repo(),
        workspace_incarnation: incarnation(),
    }
}

fn admission(job_id: u64) -> CommitmentDraft {
    CommitmentDraft::Admission {
        repo_id: repo(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(job_id).unwrap(),
        grant_revision: 3,
    }
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

fn read_segment(path: &Path) -> Vec<ControllerCommitment> {
    let file = File::open(path).unwrap();
    let mut reader = StreamReader::try_new(file, None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none(), "one batch per segment");
    assert_eq!(batch.schema(), controller_commitment_schema());
    decode_controller_commitments(&batch).unwrap()
}

#[test]
fn each_record_is_one_sealed_segment_with_the_exact_schema_and_a_writer_local_order() {
    let root = TempRoot::new("round-trip");
    let mut sink = sink_with(
        root.path(),
        FixedEnvironment {
            date: date(2026, 8, 23),
            fail_at: None,
            count_syncs: false,
        },
    );
    assert_eq!(sink.next_order(), 1);
    sink.record(introduced()).unwrap();
    sink.record(admission(1)).unwrap();
    sink.record(admission(2)).unwrap();
    assert_eq!(sink.next_order(), 4);

    let segments = sealed_segments(root.path());
    assert_eq!(segments.len(), 3);
    let writer = sink.writer_id().hyphenated().to_string();
    for (index, segment) in segments.iter().enumerate() {
        let order = index as u64 + 1;
        assert_eq!(
            segment.file_name().unwrap().to_str().unwrap(),
            format!("commitment-{order:020}-{writer}.arrow")
        );
        let records = read_segment(segment);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].order(), order);
    }
    assert!(matches!(
        read_segment(&segments[0])[0],
        ControllerCommitment::WorkspaceIntroduced(_)
    ));
    assert!(matches!(
        read_segment(&segments[2])[0],
        ControllerCommitment::Admission(_)
    ));
}

#[test]
fn two_writers_never_contend_and_keep_their_own_sequences() {
    let root = TempRoot::new("two-writers");
    let mut first = sink_with(
        root.path(),
        FixedEnvironment {
            date: date(2026, 8, 23),
            fail_at: None,
            count_syncs: false,
        },
    );
    let mut second = sink_with(
        root.path(),
        FixedEnvironment {
            date: date(2026, 8, 23),
            fail_at: None,
            count_syncs: false,
        },
    );
    assert_ne!(first.writer_id(), second.writer_id());
    first.record(introduced()).unwrap();
    second.record(introduced()).unwrap();
    first.record(admission(1)).unwrap();
    assert_eq!((first.next_order(), second.next_order()), (3, 2));
    assert_eq!(sealed_segments(root.path()).len(), 3);
}

#[test]
fn failure_before_rename_cleans_the_temporary_and_does_not_advance() {
    let root = TempRoot::new("before-rename");
    let partition = date(2026, 8, 9);
    let mut sink = sink_with(
        root.path(),
        FixedEnvironment {
            date: partition,
            fail_at: Some(CommitmentPublicationPoint::BeforeRename),
            count_syncs: false,
        },
    );
    assert!(matches!(
        sink.record(introduced()),
        Err(AuditSinkError::Io { .. })
    ));
    assert_eq!(sink.next_order(), 1);
    assert!(sealed_segments(root.path()).is_empty());
    let entries: Vec<_> = fs::read_dir(root.path().join(partition.to_string()))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(entries.is_empty(), "the temporary was unlinked");
}

#[test]
fn failure_after_rename_leaves_the_sealed_segment_and_does_not_advance() {
    let root = TempRoot::new("after-rename");
    let mut sink = sink_with(
        root.path(),
        FixedEnvironment {
            date: date(2026, 9, 10),
            fail_at: Some(CommitmentPublicationPoint::AfterRenameAndDirectorySync),
            count_syncs: false,
        },
    );
    assert!(matches!(
        sink.record(introduced()),
        Err(AuditSinkError::Io { .. })
    ));
    // The segment is sealed on disk; the in-memory sequence did not advance, so the next record
    // would collide on the same name — and rename-without-replace refuses it rather than
    // overwriting sealed history.
    assert_eq!(sink.next_order(), 1);
    assert_eq!(sealed_segments(root.path()).len(), 1);
    let mut healthy = sink_with(
        root.path(),
        FixedEnvironment {
            date: date(2026, 9, 10),
            fail_at: None,
            count_syncs: false,
        },
    );
    healthy.record(introduced()).unwrap();
    assert_eq!(sealed_segments(root.path()).len(), 2);
}

#[test]
fn sealed_segment_is_private_and_its_directories_are_synced() {
    SYNC_CALLS.store(0, Ordering::SeqCst);
    let root = TempRoot::new("mode-and-sync");
    let mut sink = sink_with(
        root.path(),
        FixedEnvironment {
            date: date(2026, 10, 11),
            fail_at: None,
            count_syncs: true,
        },
    );
    sink.record(introduced()).unwrap();
    let segments = sealed_segments(root.path());
    assert_eq!(segments.len(), 1);
    let metadata = fs::metadata(&segments[0]).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(metadata.nlink(), 1);
    assert_eq!(
        SYNC_CALLS.load(Ordering::SeqCst),
        2,
        "root synced once for the new date directory, the date directory once per record"
    );
}

#[test]
fn off_sink_writes_nothing_and_the_publisher_still_acknowledges() {
    let root = TempRoot::new("off");
    let mut off = NullAuditSink;
    off.record(introduced()).unwrap();
    assert!(!root.path().exists());
    assert_eq!(off.name(), "off");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut publisher =
            CommitmentPublisher::open(root.path(), ContinuityAudit::Off, 2).unwrap();
        publisher.record(introduced()).await.unwrap();
        publisher.record(admission(1)).await.unwrap();
        let health = publisher.health().await.unwrap();
        assert_eq!((health.sink, health.recorded, health.failed), ("off", 2, 0));
        assert!(!root.path().exists(), "off creates no telemetry root");
    });
}

struct Recording {
    seen: Arc<Mutex<Vec<ControllerCommitment>>>,
    fail_on: Option<u64>,
    next: u64,
}

impl AuditSink for Recording {
    fn record(&mut self, draft: CommitmentDraft) -> Result<(), AuditSinkError> {
        let order = self.next;
        self.next += 1;
        if self.fail_on == Some(order) {
            return Err(AuditSinkError::Integrity {
                message: format!("injected refusal of record {order}"),
            });
        }
        self.seen.lock().unwrap().push(draft.into_commitment(order));
        Ok(())
    }

    fn name(&self) -> &'static str {
        "recording"
    }
}

#[test]
fn an_injected_sink_receives_every_record_and_its_refusals_become_health_not_errors() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Recording {
        seen: Arc::clone(&seen),
        fail_on: Some(2),
        next: 1,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut publisher = CommitmentPublisher::start(Box::new(sink), 4).unwrap();
        publisher.record(introduced()).await.unwrap();
        // The second record is refused by the sink; the publisher still acknowledges — the
        // controller act already happened — and accounts for it.
        publisher.record(admission(1)).await.unwrap();
        publisher.record(admission(2)).await.unwrap();
        let health = publisher.health().await.unwrap();
        assert_eq!(health.sink, "recording");
        assert_eq!((health.recorded, health.failed), (2, 1));
        assert_eq!(
            health.last_failure.as_deref(),
            Some("audit sink integrity failure: injected refusal of record 2")
        );
    });
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(matches!(
        seen[0],
        ControllerCommitment::WorkspaceIntroduced(_)
    ));
    assert!(matches!(&seen[1], ControllerCommitment::Admission(value) if value.order == 3));
}

#[test]
fn continuity_audit_reads_its_environment_variable_and_refuses_unknown_values() {
    // The variable is process-global; serialize through a single test body.
    unsafe {
        std::env::remove_var("COWSHED_CONTINUITY_AUDIT");
    }
    assert!(matches!(
        ContinuityAudit::from_environment().unwrap(),
        ContinuityAudit::Arrow
    ));
    unsafe {
        std::env::set_var("COWSHED_CONTINUITY_AUDIT", "off");
    }
    assert!(matches!(
        ContinuityAudit::from_environment().unwrap(),
        ContinuityAudit::Off
    ));
    unsafe {
        std::env::set_var("COWSHED_CONTINUITY_AUDIT", "parquet");
    }
    let error = ContinuityAudit::from_environment().unwrap_err().to_string();
    assert!(error.contains("`arrow` (default) and `off`"), "{error}");
    unsafe {
        std::env::remove_var("COWSHED_CONTINUITY_AUDIT");
    }
}
