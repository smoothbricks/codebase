use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::api::{
    AdmissionCommitment, BinaryData, CONTROLLER_COMMITMENT_VERSION, ControllerCommitment, JobId,
    JobState, MAX_INLINE_OUTPUT_BYTES, OutputStorage, OutputSummary, ProtectedOutput, Sha256Digest,
    StreamInfo, TerminalCommitment, WorkspaceIncarnation, WorkspacePath,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::job_artifact::{
    ArtifactConfig, ArtifactError, ArtifactStore, CommitmentPriorContext, OutputTargets,
    ProtectedRecord, StreamTarget, controller_commitment_schema, controller_commitments_from_batch,
    controller_commitments_to_batch, read_stream, reconcile_commitments, recover_records,
    validate_commitments,
};
use proptest::prelude::*;

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-job-artifacts-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create temp root");
        Self(root)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = make_writable_recursive(&self.0);
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn make_writable_recursive(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        if entry.file_type()?.is_dir() {
            make_writable_recursive(&child)?;
        } else {
            let mut permissions = fs::metadata(&child)?.permissions();
            permissions.set_mode(permissions.mode() | 0o200);
            fs::set_permissions(&child, permissions)?;
        }
    }
    Ok(())
}

fn store(root: &Path, config: ArtifactConfig) -> ArtifactStore {
    ArtifactStore::open(
        root,
        RepoId::parse("acme/widget").unwrap(),
        WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
        config,
    )
    .unwrap()
}

fn summary(text: &str) -> OutputSummary {
    OutputSummary {
        version: 1,
        text: text.into(),
        truncated: false,
    }
}

#[test]
fn threshold_crossing_spills_only_the_crossing_stream_and_round_trips_arrow() {
    let root = TempRoot::new("threshold");
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 4,
            supervisor_buffer_budget_bytes: 64,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = store.start_job(7, OutputTargets::default()).unwrap();
    writer.write_stdout(b"four").unwrap();
    writer.write_stderr(b"spill").unwrap();
    let sealed = writer
        .seal(JobState::Exited, summary("stdout"), summary("stderr"))
        .unwrap();

    assert!(matches!(
        sealed.record.stdout.storage.artifact(),
        ProtectedOutput::Inline { .. }
    ));
    assert!(matches!(
        sealed.record.stderr.storage.artifact(),
        ProtectedOutput::File { .. }
    ));
    assert!(!root.path().join(".cowshed/job/1/out").exists());
    assert!(root.path().join(".cowshed/job/1/err").exists());
    assert_eq!(
        fs::metadata(root.path().join(".cowshed/job"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(root.path().join(".cowshed/job/1"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(root.path().join(".cowshed/job/1/err"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o400
    );
    assert_eq!(
        fs::metadata(root.path().join(".cowshed/job/records.arrow"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        read_stream(root.path(), &sealed.record.stdout).unwrap(),
        b"four"
    );
    assert_eq!(
        read_stream(root.path(), &sealed.record.stderr).unwrap(),
        b"spill"
    );
    assert_eq!(store.buffered_bytes(), 0);

    let recovered = recover_records(&root.path().join(".cowshed/job/records.arrow")).unwrap();
    assert_eq!(recovered.frames.len(), 2);
    assert!(matches!(
        &recovered.frames[1].record,
        ProtectedRecord::Job(record) if record == &sealed.record
    ));
}

#[test]
fn supervisor_budget_is_shared_but_stdout_and_stderr_transition_independently() {
    let root = TempRoot::new("budget");
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 10,
            supervisor_buffer_budget_bytes: 4,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
    writer.write_stdout(b"1234").unwrap();
    assert_eq!(store.buffered_bytes(), 4);
    writer.write_stderr(b"x").unwrap();
    assert_eq!(store.buffered_bytes(), 4);
    let sealed = writer
        .seal(JobState::Exited, summary(""), summary(""))
        .unwrap();
    assert!(matches!(
        sealed.record.stdout.storage.artifact(),
        ProtectedOutput::Inline { .. }
    ));
    assert!(matches!(
        sealed.record.stderr.storage.artifact(),
        ProtectedOutput::File { .. }
    ));

    assert_eq!(store.buffered_bytes(), 0);
}
#[test]
fn many_tiny_live_buffers_never_exceed_supervisor_retained_budget() {
    let root = TempRoot::new("many-tiny");
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 64,
            supervisor_buffer_budget_bytes: 16,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let mut writers = Vec::new();
    for _ in 0..64 {
        let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
        writer.write_stdout(b"x").unwrap();
        assert!(store.buffered_bytes() <= 16);
        writers.push(writer);
    }
    drop(writers);
    assert_eq!(store.buffered_bytes(), 0);
}


#[test]
fn combined_quota_accepts_exact_boundary_and_reports_first_crossing() {
    let exact_root = TempRoot::new("quota-exact");
    let exact = store(
        exact_root.path(),
        ArtifactConfig {
            inline_cap_bytes: 16,
            supervisor_buffer_budget_bytes: 16,
            combined_output_quota_bytes: 5,
            ..ArtifactConfig::default()
        },
    );
    let mut exact_writer = exact.start_job(1, OutputTargets::default()).unwrap();
    exact_writer.write_stdout(b"123").unwrap();
    exact_writer.write_stderr(b"45").unwrap();
    assert!(exact_writer.output_limit().is_none());
    let exact_sealed = exact_writer
        .seal(JobState::Exited, summary(""), summary(""))
        .unwrap();
    assert_eq!(
        exact_sealed.record.stdout.bytes + exact_sealed.record.stderr.bytes,
        5
    );

    let crossed_root = TempRoot::new("quota-crossed");
    let crossed = store(
        crossed_root.path(),
        ArtifactConfig {
            inline_cap_bytes: 16,
            supervisor_buffer_budget_bytes: 16,
            combined_output_quota_bytes: 5,
            ..ArtifactConfig::default()
        },
    );
    let mut crossed_writer = crossed.start_job(1, OutputTargets::default()).unwrap();
    crossed_writer.write_stdout(b"123").unwrap();
    let error = crossed_writer.write_stderr(b"456").unwrap_err();
    assert!(matches!(
        error,
        ArtifactError::OutputQuotaExceeded {
            limit_bytes: 5,
            crossing_bytes: 6
        }
    ));
    assert_eq!(crossed_writer.output_limit().unwrap().crossing_bytes, 6);
    assert!(matches!(
        crossed_writer.write_stdout(b"ignored").unwrap_err(),
        ArtifactError::OutputQuotaExceeded {
            limit_bytes: 5,
            crossing_bytes: 6
        }
    ));
    let sealed = crossed_writer
        .seal(JobState::OutputLimit, summary(""), summary(""))
        .unwrap();
    assert_eq!(sealed.record.stdout.bytes + sealed.record.stderr.bytes, 5);
    assert_eq!(
        read_stream(crossed_root.path(), &sealed.record.stderr).unwrap(),
        b"45"
    );
}

#[test]
fn invalid_utf8_is_tagged_base64_and_survives_json_and_arrow() {
    let bytes = vec![0xff, 0x00, b'a', 0x80];
    let binary = BinaryData::new(bytes.clone()).unwrap();
    let json = serde_json::to_value(&binary).unwrap();
    assert_eq!(json["encoding"], "base64");
    assert_eq!(serde_json::from_value::<BinaryData>(json).unwrap(), binary);
    let utf8_json = serde_json::to_value(BinaryData::new(b"hello".to_vec()).unwrap()).unwrap();
    assert_eq!(utf8_json["encoding"], "utf8");
    assert!(BinaryData::new(vec![0; MAX_INLINE_OUTPUT_BYTES + 1]).is_err());

    let root = TempRoot::new("binary");
    let store = store(root.path(), ArtifactConfig::default());
    let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
    writer.write_stdout(&bytes).unwrap();
    let sealed = writer
        .seal(JobState::Exited, summary(""), summary(""))
        .unwrap();
    let stream_json = serde_json::to_value(&sealed.record.stdout).unwrap();
    assert_eq!(
        stream_json["storage"]["artifact"]["data"]["encoding"],
        "base64"
    );
    let mut invalid_stream_json = stream_json.clone();
    invalid_stream_json["bytes"] = serde_json::json!(bytes.len() + 1);
    assert!(serde_json::from_value::<StreamInfo>(invalid_stream_json).is_err());
    assert_eq!(
        read_stream(root.path(), &sealed.record.stdout).unwrap(),
        bytes
    );
    let recovered = recover_records(&root.path().join(".cowshed/job/records.arrow")).unwrap();
    assert!(matches!(
        &recovered.frames[1].record,
        ProtectedRecord::Job(record) if record.stdout == sealed.record.stdout
    ));
}

#[test]
fn restart_allocates_after_inline_only_record_without_numeric_directory() {
    let root = TempRoot::new("restart");
    let first = store(root.path(), ArtifactConfig::default());
    let writer = first.start_job(1, OutputTargets::default()).unwrap();
    assert_eq!(writer.job_id().get(), 1);
    writer
        .seal(JobState::Exited, summary(""), summary(""))
        .unwrap();
    assert!(!root.path().join(".cowshed/job/1").exists());
    drop(first);

    let restarted = store(root.path(), ArtifactConfig::default());
    assert_eq!(restarted.recovery().next_job_id.get(), 2);
    let second = restarted.start_job(1, OutputTargets::default()).unwrap();
    assert_eq!(second.job_id().get(), 2);
}

#[test]
fn redirect_sealing_uses_owned_descriptor_and_reads_only_independent_artifact() {
    let root = TempRoot::new("redirect");
    let source = WorkspacePath::new("build/live.log").unwrap();
    fs::create_dir_all(root.path().join("build")).unwrap();
    let descriptor = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(root.path().join(source.as_path()))
        .unwrap();
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 2,
            supervisor_buffer_budget_bytes: 2,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = store
        .start_job(
            1,
            OutputTargets {
                stdout: StreamTarget::Redirect {
                    source: source.clone(),
                    descriptor,
                },
                stderr: StreamTarget::Captured,
            },
        )
        .unwrap();
    writer.write_stdout(b"protected").unwrap();
    let sealed = writer
        .seal(JobState::Exited, summary(""), summary(""))
        .unwrap();
    assert!(matches!(
        sealed.record.stdout.storage,
        OutputStorage::Redirect {
            artifact: ProtectedOutput::File { .. },
            ..
        }
    ));

    fs::write(root.path().join(source.as_path()), b"mutated caller file").unwrap();
    assert_eq!(
        read_stream(root.path(), &sealed.record.stdout).unwrap(),
        b"protected"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let source_metadata = fs::metadata(root.path().join(source.as_path())).unwrap();
        let protected_metadata = fs::metadata(root.path().join(".cowshed/job/1/out")).unwrap();
        assert_ne!(source_metadata.ino(), protected_metadata.ino());
        fs::hard_link(
            root.path().join(".cowshed/job/1/out"),
            root.path().join("workspace-alias"),
        )
        .unwrap();
        assert!(matches!(
            read_stream(root.path(), &sealed.record.stdout).unwrap_err(),
            ArtifactError::Integrity { .. }
        ));
    }
}

#[test]
fn recovery_truncates_only_incomplete_tail_and_rejects_complete_corruption() {
    let root = TempRoot::new("recovery-tail");
    let store = store(root.path(), ArtifactConfig::default());
    store
        .start_job(1, OutputTargets::default())
        .unwrap()
        .seal(JobState::Exited, summary(""), summary(""))
        .unwrap();
    let records = root.path().join(".cowshed/job/records.arrow");
    let valid_len = fs::metadata(&records).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&records)
        .unwrap()
        .write_all(b"CSBAT")
        .unwrap();
    let recovered = recover_records(&records).unwrap();
    assert_eq!(recovered.truncated_bytes, 5);
    assert_eq!(fs::metadata(&records).unwrap().len(), valid_len);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&records)
        .unwrap();
    file.seek(SeekFrom::Start(8 + 24 + 12)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    assert!(matches!(
        recover_records(&records).unwrap_err(),
        ArtifactError::Integrity { .. }
    ));
}

#[test]
fn commitment_continuity_and_terminal_reconciliation_are_enforced() {
    let root = TempRoot::new("commitment-reconcile");
    let store = store(root.path(), ArtifactConfig::default());
    let mut writer = store.start_job(7, OutputTargets::default()).unwrap();
    writer.write_stdout(b"authority").unwrap();
    let sealed = writer
        .seal(JobState::Exited, summary(""), summary(""))
        .unwrap();
    let repo_id = RepoId::parse("acme/widget").unwrap();
    let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
    let commitments = [
        ControllerCommitment::Admission(AdmissionCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 1,
            repo_id: repo_id.clone(),
            workspace_incarnation: incarnation.clone(),
            job_id: sealed.record.job_id,
            grant_revision: 7,
        }),
        ControllerCommitment::Terminal(TerminalCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 2,
            repo_id: repo_id.clone(),
            workspace_incarnation: incarnation.clone(),
            job_id: sealed.record.job_id,
            state: sealed.record.state,
            grant_revision: sealed.record.grant_revision,
            stdout_bytes: sealed.record.stdout.bytes,
            stdout_sha256: sealed.record.stdout.sha256,
            stderr_bytes: sealed.record.stderr.bytes,
            stderr_sha256: sealed.record.stderr.sha256,
            batch_sha256: sealed.terminal_batch_sha256,
        }),
    ];
    let prior = CommitmentPriorContext::new(repo_id.clone(), [incarnation.clone()]);
    let recovery = recover_records(&root.path().join(".cowshed/job/records.arrow")).unwrap();
    assert_eq!(
        reconcile_commitments(&recovery, &prior, &commitments)
            .unwrap()
            .last_order(),
        2
    );

    let gap = ControllerCommitment::Admission(AdmissionCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order: 2,
        repo_id,
        workspace_incarnation: incarnation,
        job_id: JobId::new(2).unwrap(),
        grant_revision: 1,
    });
    assert!(matches!(
        validate_commitments(&prior, &[gap]),
        Err(ArtifactError::Integrity { .. })
    ));

    let mut mismatched = commitments;
    if let ControllerCommitment::Terminal(terminal) = &mut mismatched[1] {
        terminal.stdout_bytes += 1;
    }
    assert!(matches!(
        reconcile_commitments(&recovery, &prior, &mismatched),
        Err(ArtifactError::Integrity { .. })
    ));
}

#[test]
fn controller_commitment_arrow_is_payload_free_and_round_trips_lineage() {
    let digest = Sha256Digest::compute(b"digest");
    let value = ControllerCommitment::Terminal(TerminalCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order: 9,
        repo_id: RepoId::parse("acme/widget").unwrap(),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .unwrap(),
        job_id: JobId::new(2).unwrap(),
        state: JobState::Exited,
        grant_revision: 7,
        stdout_bytes: 11,
        stdout_sha256: digest,
        stderr_bytes: 0,
        stderr_sha256: Sha256Digest::compute(&[]),
        batch_sha256: digest,
    });
    let batch = controller_commitments_to_batch(std::slice::from_ref(&value)).unwrap();
    assert_eq!(
        controller_commitments_from_batch(&batch).unwrap(),
        vec![value.clone()]
    );
    let schema = controller_commitment_schema();
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert!(!names.iter().any(|name| {
        name.contains("inline") || name.contains("path") || name.contains("payload")
    }));
    let json = serde_json::to_string(&value).unwrap();
    for forbidden in ["artifact", "inline", "path", "payload", "data"] {
        assert!(
            !json.contains(forbidden),
            "controller JSON leaked {forbidden}"
        );
    }
}

proptest! {
    #[test]
    fn binary_json_round_trip_is_exact_and_bounded(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let value = BinaryData::new(bytes.clone()).unwrap();
        let encoded = serde_json::to_vec(&value).unwrap();
        let decoded: BinaryData = serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(decoded.as_bytes(), bytes.as_slice());
        prop_assert!(encoded.len() <= bytes.len().saturating_mul(2).saturating_add(64));
    }
}
