use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::api::{
    AdmissionCommitment, BinaryData, CONTROLLER_COMMITMENT_VERSION, CheckpointCommitment,
    ControllerCommitment, JobId, JobState, MAX_INLINE_OUTPUT_BYTES, OutputLimitInfo,
    OutputPublication, OutputStorage, ProtectedOutput, PublicationPolicy, Sha256Digest, StreamInfo,
    TerminalCommitment, WorkspaceIncarnation, WorkspacePath,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::job_artifact::{
    ArtifactConfig, ArtifactError, ArtifactStore, CommitmentPriorContext, OutputTargets,
    ProtectedRecord, PublicationStage, StreamKind, StreamTarget, controller_commitment_schema,
    controller_commitments_from_batch, controller_commitments_to_batch, open_stream_reader,
    read_stream, reconcile_commitments, recover_records, recover_records_with_budget,
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

#[test]
fn invalid_configs_are_rejected_before_creating_protected_layout() {
    let cases = [
        ArtifactConfig {
            inline_cap_bytes: MAX_INLINE_OUTPUT_BYTES + 1,
            ..ArtifactConfig::default()
        },
        ArtifactConfig {
            combined_output_quota_bytes: 0,
            ..ArtifactConfig::default()
        },
        ArtifactConfig {
            retained_recovery_budget_bytes: 0,
            ..ArtifactConfig::default()
        },
    ];
    for (index, config) in cases.into_iter().enumerate() {
        let root = TempRoot::new(&format!("invalid-config-{index}"));
        assert!(matches!(
            ArtifactStore::open(
                root.path(),
                RepoId::parse("acme/widget").unwrap(),
                WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
                config,
            ),
            Err(ArtifactError::InvalidConfig(_))
        ));
        assert!(
            !root.path().join(".cowshed").exists(),
            "configuration validation must precede layout creation"
        );
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
    let sealed = writer.seal(JobState::Exited).unwrap();

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
    let sealed = writer.seal(JobState::Exited).unwrap();
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
    let exact_sealed = exact_writer.seal(JobState::Exited).unwrap();
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
    let sealed = crossed_writer.seal(JobState::OutputLimit).unwrap();
    assert_eq!(sealed.record.stdout.bytes + sealed.record.stderr.bytes, 5);
    assert_eq!(sealed.record.output_limit, sealed.output_limit);
    assert_eq!(sealed.record.output_limit.as_ref().unwrap().limit_bytes, 5);
    assert_eq!(
        sealed.record.output_limit.as_ref().unwrap().crossing_bytes,
        6
    );
    let recovered =
        recover_records(&crossed_root.path().join(".cowshed/job/records.arrow")).unwrap();
    assert!(matches!(
        &recovered.frames.last().unwrap().record,
        ProtectedRecord::Job(record) if record.output_limit == sealed.record.output_limit
    ));
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
    let sealed = writer.seal(JobState::Exited).unwrap();
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
    writer.seal(JobState::Exited).unwrap();
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
    use std::os::fd::AsRawFd;
    let redirect_fd = descriptor.as_raw_fd();
    assert_eq!(
        unsafe { libc::fcntl(redirect_fd, libc::F_SETFD, 0) },
        0,
        "clear the standard library's default close-on-exec flag"
    );
    assert_eq!(
        unsafe { libc::fcntl(redirect_fd, libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
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
    assert_ne!(
        unsafe { libc::fcntl(redirect_fd, libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
    writer.write_stdout(b"protected").unwrap();
    fs::write(root.path().join(source.as_path()), b"mutated before seal").unwrap();
    let sealed = writer.seal(JobState::Exited).unwrap();
    assert!(matches!(
        sealed.record.stdout.storage,
        OutputStorage::Redirect {
            artifact: ProtectedOutput::File { .. },
            ..
        }
    ));

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
        .seal(JobState::Exited)
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
    let sealed = writer.seal(JobState::Exited).unwrap();
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
            output_limit: sealed.record.output_limit.clone(),
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

    for mutation in 0..7 {
        let mut mismatched = commitments.clone();
        let ControllerCommitment::Terminal(terminal) = &mut mismatched[1] else {
            unreachable!()
        };
        match mutation {
            0 => terminal.stdout_bytes += 1,
            1 => terminal.stdout_sha256 = Sha256Digest::compute(b"wrong-stdout"),
            2 => terminal.stderr_bytes += 1,
            3 => terminal.stderr_sha256 = Sha256Digest::compute(b"wrong-stderr"),
            4 => terminal.batch_sha256 = Sha256Digest::compute(b"wrong-batch"),
            5 => terminal.state = JobState::Signaled,
            6 => terminal.grant_revision += 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            reconcile_commitments(&recovery, &prior, &mismatched),
            Err(ArtifactError::Integrity { .. })
        ));
    }
}

#[test]
fn checkpoint_reconciliation_rejects_altered_manifest_fields() {
    let root = TempRoot::new("checkpoint-reconcile");
    let store = store(root.path(), ArtifactConfig::default());
    let sealed = store.checkpoint(1).unwrap();
    let recovery = recover_records(&root.path().join(".cowshed/job/records.arrow")).unwrap();
    let repo_id = RepoId::parse("acme/widget").unwrap();
    let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
    let prior = CommitmentPriorContext::new(repo_id.clone(), [incarnation.clone()]);
    let commitment = ControllerCommitment::Checkpoint(CheckpointCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order: 1,
        repo_id,
        origin_incarnation: incarnation,
        checkpoint_id: "checkpoint-one".into(),
        barrier_id: 1,
        manifest_batch_sha256: sealed.manifest_batch_sha256,
    });
    reconcile_commitments(&recovery, &prior, std::slice::from_ref(&commitment)).unwrap();

    for mutation in 0..2 {
        let mut altered = commitment.clone();
        let ControllerCommitment::Checkpoint(checkpoint) = &mut altered else {
            unreachable!()
        };
        match mutation {
            0 => checkpoint.barrier_id = 2,
            1 => checkpoint.manifest_batch_sha256 = Sha256Digest::compute(b"altered"),
            _ => unreachable!(),
        }
        assert!(matches!(
            reconcile_commitments(&recovery, &prior, &[altered]),
            Err(ArtifactError::Integrity { .. })
        ));
    }
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
        state: JobState::OutputLimit,
        grant_revision: 7,
        stdout_bytes: 11,
        stdout_sha256: digest,
        stderr_bytes: 0,
        stderr_sha256: Sha256Digest::compute(&[]),
        batch_sha256: digest,
        output_limit: Some(OutputLimitInfo {
            limit_bytes: 10,
            crossing_bytes: 12,
        }),
    });
    let admission = ControllerCommitment::Admission(AdmissionCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order: 1,
        repo_id: RepoId::parse("acme/widget").unwrap(),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .unwrap(),
        job_id: JobId::new(2).unwrap(),
        grant_revision: 7,
    });
    let mut value = value;
    if let ControllerCommitment::Terminal(terminal) = &mut value {
        terminal.order = 2;
    }
    let values = vec![admission, value];
    let batch = controller_commitments_to_batch(&values).unwrap();
    let prior = CommitmentPriorContext::new(
        RepoId::parse("acme/widget").unwrap(),
        [WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap()],
    );
    assert_eq!(
        controller_commitments_from_batch(&batch, &prior).unwrap(),
        values
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
    let json = serde_json::to_string(&values[1]).unwrap();
    for forbidden in ["artifact", "inline", "path", "payload", "data"] {
        assert!(
            !json.contains(forbidden),
            "controller JSON leaked {forbidden}"
        );
    }
}

#[test]
fn output_publication_json_is_bounded_and_explicit() {
    let value = OutputPublication {
        path: WorkspacePath::new("published/stdout.bin").unwrap(),
        policy: PublicationPolicy::CreateNew,
    };
    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        serde_json::json!({
            "path": "published/stdout.bin",
            "policy": "createNew"
        })
    );
}

#[test]
fn sealed_output_publication_is_atomic_independent_and_policy_checked() {
    use std::os::unix::fs::MetadataExt;

    let root = TempRoot::new("publication");
    fs::create_dir(root.path().join("published")).unwrap();
    let initial_store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 4,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = initial_store
        .start_job(1, OutputTargets::default())
        .unwrap();
    writer.write_stdout(b"sealed output").unwrap();
    let sealed = writer.seal(JobState::Exited).unwrap();
    let protected = match sealed.record.stdout.storage.artifact() {
        ProtectedOutput::File { path } => root.path().join(path.as_path()),
        ProtectedOutput::Inline { .. } => panic!("expected promoted protected output"),
    };
    drop(initial_store);
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 4,
            ..ArtifactConfig::default()
        },
    );
    let publication = OutputPublication {
        path: WorkspacePath::new("published/stdout.txt").unwrap(),
        policy: PublicationPolicy::CreateNew,
    };

    store
        .publish_output(sealed.record.job_id, StreamKind::Stdout, &publication)
        .unwrap();
    let destination = root.path().join(publication.path.as_path());
    assert_eq!(fs::read(&destination).unwrap(), b"sealed output");
    assert_ne!(
        fs::metadata(&protected).unwrap().ino(),
        fs::metadata(&destination).unwrap().ino()
    );
    assert_eq!(
        fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
        0o600
    );

    fs::write(&destination, b"caller mutation").unwrap();
    assert_eq!(
        read_stream(root.path(), &sealed.record.stdout).unwrap(),
        b"sealed output"
    );
    let error = store
        .publish_output(sealed.record.job_id, StreamKind::Stdout, &publication)
        .unwrap_err();
    assert!(matches!(
        error,
        ArtifactError::Publication {
            stage: PublicationStage::Publish,
            ..
        }
    ));
    assert_eq!(fs::read(&destination).unwrap(), b"caller mutation");

    let replace = OutputPublication {
        policy: PublicationPolicy::Replace,
        ..publication
    };
    store
        .publish_output(sealed.record.job_id, StreamKind::Stdout, &replace)
        .unwrap();
    assert_eq!(fs::read(&destination).unwrap(), b"sealed output");
    assert!(
        fs::read_dir(root.path().join("published"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("cowshed-publish"))
    );
}

#[test]
fn output_publication_rejects_control_paths_symlinks_and_corrupt_evidence() {
    use std::os::unix::fs::symlink;

    let root = TempRoot::new("publication-deny");
    fs::create_dir(root.path().join("published")).unwrap();
    fs::create_dir(root.path().join("outside")).unwrap();
    symlink(root.path().join("outside"), root.path().join("linked")).unwrap();
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
    writer.write_stdout(b"authority").unwrap();
    let sealed = writer.seal(JobState::Exited).unwrap();

    for path in [".cowshed/leak", "linked/leak"] {
        let error = store
            .publish_output(
                sealed.record.job_id,
                StreamKind::Stdout,
                &OutputPublication {
                    path: WorkspacePath::new(path).unwrap(),
                    policy: PublicationPolicy::CreateNew,
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ArtifactError::Publication {
                stage: PublicationStage::ValidateDestination,
                ..
            }
        ));
    }

    let uncommitted = OutputPublication {
        path: WorkspacePath::new("published/uncommitted.txt").unwrap(),
        policy: PublicationPolicy::CreateNew,
    };
    assert!(matches!(
        store.publish_output(JobId::new(99).unwrap(), StreamKind::Stdout, &uncommitted),
        Err(ArtifactError::Publication {
            stage: PublicationStage::ValidateDestination,
            ..
        })
    ));
    assert!(!root.path().join(uncommitted.path.as_path()).exists());

    let protected = match sealed.record.stdout.storage.artifact() {
        ProtectedOutput::File { path } => root.path().join(path.as_path()),
        ProtectedOutput::Inline { .. } => panic!("expected protected file"),
    };
    fs::set_permissions(&protected, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(protected, b"tampered").unwrap();
    let destination = root.path().join("published/corrupt.txt");
    let error = store
        .publish_output(
            sealed.record.job_id,
            StreamKind::Stdout,
            &OutputPublication {
                path: WorkspacePath::new("published/corrupt.txt").unwrap(),
                policy: PublicationPolicy::CreateNew,
            },
        )
        .unwrap_err();
    assert!(matches!(error, ArtifactError::Integrity { .. }));
    assert!(!destination.exists());
    assert!(
        fs::read_dir(root.path().join("published"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("cowshed-publish"))
    );
}

#[test]
fn checkpoint_and_background_force_every_visible_prefix_to_durable_files() {
    let root = TempRoot::new("checkpoint-prefixes");
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 64,
            supervisor_buffer_budget_bytes: 128,
            ..ArtifactConfig::default()
        },
    );
    let committed = store
        .start_job(1, OutputTargets::default())
        .unwrap()
        .seal(JobState::Exited)
        .unwrap();
    let mut writer = store.start_job(2, OutputTargets::default()).unwrap();
    writer.write_stdout(b"background-prefix").unwrap();
    writer.prepare_background().unwrap();
    assert_eq!(
        fs::read(root.path().join(".cowshed/job/2/out")).unwrap(),
        b"background-prefix"
    );
    assert_eq!(
        fs::metadata(root.path().join(".cowshed/job/2/err"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o400
    );

    let checkpoint = store.checkpoint(1).unwrap();
    assert_eq!(checkpoint.record.barrier_id, 1);
    assert!(matches!(
        store.checkpoint(1),
        Err(ArtifactError::Integrity { .. })
    ));
    assert_eq!(checkpoint.record.visible_jobs.len(), 2);
    let active = checkpoint
        .record
        .visible_jobs
        .iter()
        .find(|job| job.job_id == writer.job_id())
        .unwrap();
    assert_eq!(active.stdout.bytes, b"background-prefix".len() as u64);
    assert_eq!(
        active.stdout.sha256,
        Sha256Digest::compute(b"background-prefix")
    );
    assert_eq!(active.stderr.bytes, 0);
    assert!(active.stdout.protected_path.is_some());
    assert!(active.stderr.protected_path.is_some());
    assert!(
        checkpoint
            .record
            .visible_jobs
            .iter()
            .any(|job| job.job_id == committed.record.job_id)
    );

    drop(writer);
    drop(store);
    let reopened = ArtifactStore::open(
        root.path(),
        RepoId::parse("acme/widget").unwrap(),
        WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
        ArtifactConfig::default(),
    )
    .unwrap();
    assert!(matches!(
        &reopened.recovery().frames.last().unwrap().record,
        ProtectedRecord::CheckpointManifest(manifest) if *manifest == checkpoint.record
    ));
}

#[test]
fn open_rejects_missing_corrupt_and_swapped_file_backed_streams() {
    let root = TempRoot::new("open-file-validation");
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
    writer.write_stdout(b"stdout-authority").unwrap();
    writer.write_stderr(b"err").unwrap();
    writer.seal(JobState::Exited).unwrap();
    drop(store);

    let out = root.path().join(".cowshed/job/1/out");
    let err = root.path().join(".cowshed/job/1/err");
    let saved = root.path().join(".cowshed/job/1/saved");
    fs::rename(&out, &saved).unwrap();
    assert!(matches!(
        ArtifactStore::open(
            root.path(),
            RepoId::parse("acme/widget").unwrap(),
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
            ArtifactConfig::default(),
        ),
        Err(ArtifactError::Io { .. })
    ));
    fs::rename(&saved, &out).unwrap();

    fs::set_permissions(&out, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&out, b"tampered-content").unwrap();
    fs::set_permissions(&out, fs::Permissions::from_mode(0o400)).unwrap();
    assert!(matches!(
        ArtifactStore::open(
            root.path(),
            RepoId::parse("acme/widget").unwrap(),
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
            ArtifactConfig::default(),
        ),
        Err(ArtifactError::Integrity { .. })
    ));
    fs::set_permissions(&out, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&out, b"stdout-authority").unwrap();
    fs::set_permissions(&out, fs::Permissions::from_mode(0o400)).unwrap();

    fs::rename(&out, &saved).unwrap();
    fs::rename(&err, &out).unwrap();
    fs::rename(&saved, &err).unwrap();
    assert!(matches!(
        ArtifactStore::open(
            root.path(),
            RepoId::parse("acme/widget").unwrap(),
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
            ArtifactConfig::default(),
        ),
        Err(ArtifactError::Integrity { .. })
    ));
}

#[test]
fn verified_file_rejects_each_metadata_length_and_hash_violation() {
    #[derive(Clone, Copy, Debug)]
    enum Mutation {
        WritableMode,
        Hardlink,
        Truncated,
        Extended,
        SameLengthHash,
        Symlink,
        Directory,
    }

    for mutation in [
        Mutation::WritableMode,
        Mutation::Hardlink,
        Mutation::Truncated,
        Mutation::Extended,
        Mutation::SameLengthHash,
        Mutation::Symlink,
        Mutation::Directory,
    ] {
        let root = TempRoot::new(&format!("verified-file-{mutation:?}"));
        let store = store(
            root.path(),
            ArtifactConfig {
                inline_cap_bytes: 1,
                ..ArtifactConfig::default()
            },
        );
        let expected = b"sealed-authority";
        let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
        writer.write_stdout(expected).unwrap();
        let sealed = writer.seal(JobState::Exited).unwrap();
        let path = root.path().join(".cowshed/job/1/out");

        {
            use std::os::unix::fs::MetadataExt;
            let metadata = fs::metadata(&path).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o400);
            assert_eq!(metadata.nlink(), 1);
            assert_eq!(metadata.len(), expected.len() as u64);
        }
        assert_eq!(
            read_stream(root.path(), &sealed.record.stdout).unwrap(),
            expected
        );

        match mutation {
            Mutation::WritableMode => {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            }
            Mutation::Hardlink => {
                fs::hard_link(&path, root.path().join("protected-alias")).unwrap();
            }
            Mutation::Truncated => {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len((expected.len() - 1) as u64)
                    .unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
            }
            Mutation::Extended => {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
                OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap()
                    .write_all(b"+")
                    .unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
            }
            Mutation::SameLengthHash => {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
                let mut file = OpenOptions::new().write(true).open(&path).unwrap();
                file.write_all(b"X").unwrap();
                file.sync_all().unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
            }
            Mutation::Symlink => {
                use std::os::unix::fs::symlink;

                let backing = root.path().join(".cowshed/job/1/backing");
                fs::rename(&path, &backing).unwrap();
                symlink("backing", &path).unwrap();
            }
            Mutation::Directory => {
                fs::remove_file(&path).unwrap();
                fs::create_dir(&path).unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
            }
        }

        let error = read_stream(root.path(), &sealed.record.stdout).unwrap_err();
        match mutation {
            Mutation::WritableMode | Mutation::Directory => assert!(
                matches!(
                    &error,
                    ArtifactError::Integrity { message, .. } if message.contains("sealed regular file")
                ),
                "{mutation:?}: {error:?}"
            ),
            Mutation::Hardlink => assert!(matches!(
                error,
                ArtifactError::Integrity { message, .. } if message.contains("hardlink aliases")
            )),
            Mutation::Truncated | Mutation::Extended => assert!(matches!(
                error,
                ArtifactError::Integrity { message, .. } if message.contains("length differs")
            )),
            Mutation::SameLengthHash => assert!(matches!(
                error,
                ArtifactError::Integrity { message, .. } if message.contains("does not match")
            )),
            Mutation::Symlink => assert!(matches!(error, ArtifactError::Io { .. })),
        }
        if matches!(mutation, Mutation::Directory) {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
}

#[test]
fn verified_open_rejects_a_symlinked_workspace_root() {
    use std::os::unix::fs::symlink;

    let container = TempRoot::new("symlinked-workspace-root");
    let actual = container.path().join("actual");
    fs::create_dir(&actual).unwrap();
    let store = store(
        &actual,
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
    writer.write_stdout(b"protected").unwrap();
    let sealed = writer.seal(JobState::Exited).unwrap();
    let alias = container.path().join("alias");
    symlink(&actual, &alias).unwrap();

    assert!(matches!(
        read_stream(&alias, &sealed.record.stdout),
        Err(ArtifactError::Io { .. }) | Err(ArtifactError::Integrity { .. })
    ));
}

#[test]
fn streaming_recovery_enforces_retained_budget_before_payload_allocation() {
    let root = TempRoot::new("recovery-budget");
    let store = store(root.path(), ArtifactConfig::default());
    store.start_job(1, OutputTargets::default()).unwrap();
    let records = root.path().join(".cowshed/job/records.arrow");
    let error = recover_records_with_budget(&records, 1).unwrap_err();
    assert!(matches!(
        error,
        ArtifactError::RecoveryBudgetExceeded {
            limit_bytes: 1,
            required_bytes
        } if required_bytes > 1
    ));
}

#[test]
fn completion_seals_before_independent_publication_outcomes() {
    let root = TempRoot::new("seal-publish");
    fs::create_dir(root.path().join("published")).unwrap();
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let mut writer = store.start_job(5, OutputTargets::default()).unwrap();
    writer.write_stdout(b"stdout").unwrap();
    writer.write_stderr(b"stderr").unwrap();
    let completed = writer
        .seal_and_publish(
            JobState::Exited,
            Some(OutputPublication {
                path: WorkspacePath::new(".cowshed/forbidden").unwrap(),
                policy: PublicationPolicy::CreateNew,
            }),
            Some(OutputPublication {
                path: WorkspacePath::new("published/stderr").unwrap(),
                policy: PublicationPolicy::CreateNew,
            }),
        )
        .unwrap();
    assert!(matches!(
        completed.stdout_publication,
        Some(Err(ArtifactError::Publication {
            stage: PublicationStage::ValidateDestination,
            ..
        }))
    ));
    assert_eq!(completed.stderr_publication, Some(Ok(())));
    assert_eq!(
        fs::read(root.path().join("published/stderr")).unwrap(),
        b"stderr"
    );
    assert_eq!(completed.sealed.record.state, JobState::Exited);
    assert!(completed.sealed.record.stdout.summary.text.is_empty());
    assert!(completed.sealed.record.stderr.summary.text.is_empty());
    let recovery = recover_records(&root.path().join(".cowshed/job/records.arrow")).unwrap();
    assert!(recovery.frames.iter().any(|frame| {
        matches!(
            &frame.record,
            ProtectedRecord::Job(record)
                if record.job_id == completed.sealed.record.job_id
                    && record.state == JobState::Exited
        )
    }));
}

#[test]
fn incremental_reader_verifies_large_files_while_vec_convenience_is_bounded() {
    let root = TempRoot::new("incremental-reader");
    let store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let bytes = vec![0x5a; MAX_INLINE_OUTPUT_BYTES + 1];
    let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
    writer.write_stdout(&bytes).unwrap();
    let sealed = writer.seal(JobState::Exited).unwrap();
    assert!(matches!(
        read_stream(root.path(), &sealed.record.stdout),
        Err(ArtifactError::StreamTooLarge {
            limit_bytes,
            bytes
        }) if limit_bytes == MAX_INLINE_OUTPUT_BYTES as u64
            && bytes == MAX_INLINE_OUTPUT_BYTES as u64 + 1
    ));

    let mut reader = open_stream_reader(root.path(), &sealed.record.stdout).unwrap();
    let mut chunk = [0_u8; 777];
    let mut observed = 0_usize;
    loop {
        let read = reader.read_chunk(&mut chunk).unwrap();
        if read == 0 {
            break;
        }
        assert!(chunk[..read].iter().all(|byte| *byte == 0x5a));
        observed += read;
    }
    assert_eq!(observed, bytes.len());
    assert!(reader.is_verified());

    let path = match sealed.record.stdout.storage.artifact() {
        ProtectedOutput::File { path } => root.path().join(path.as_path()),
        ProtectedOutput::Inline { .. } => unreachable!(),
    };
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start((MAX_INLINE_OUTPUT_BYTES / 2) as u64))
        .unwrap();
    file.write_all(&[0x00]).unwrap();
    file.sync_all().unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
    assert!(matches!(
        open_stream_reader(root.path(), &sealed.record.stdout)
            .unwrap()
            .finish(),
        Err(ArtifactError::Integrity { .. })
    ));
}

#[test]
fn verified_reader_bounds_chunks_and_finish_consumes_the_exact_remainder() {
    for inline_cap_bytes in [64, 1] {
        let root = TempRoot::new(&format!("reader-bound-{inline_cap_bytes}"));
        let store = store(
            root.path(),
            ArtifactConfig {
                inline_cap_bytes,
                ..ArtifactConfig::default()
            },
        );
        let expected = b"abcdef";
        let mut writer = store.start_job(1, OutputTargets::default()).unwrap();
        writer.write_stdout(expected).unwrap();
        let sealed = writer.seal(JobState::Exited).unwrap();

        let mut partial = open_stream_reader(root.path(), &sealed.record.stdout).unwrap();
        assert_eq!(partial.read_chunk(&mut []).unwrap(), 0);
        assert!(!partial.is_verified());
        let mut bounded = [0xa5; 3];
        assert_eq!(partial.read_chunk(&mut bounded[..2]).unwrap(), 2);
        assert_eq!(&bounded[..2], b"ab");
        assert_eq!(bounded[2], 0xa5);
        partial.finish().unwrap();

        let mut exact = open_stream_reader(root.path(), &sealed.record.stdout).unwrap();
        let mut observed = Vec::new();
        let mut chunk = [0_u8; 2];
        for expected_read in [2, 2, 2, 0] {
            let read = exact.read_chunk(&mut chunk).unwrap();
            assert_eq!(read, expected_read);
            observed.extend_from_slice(&chunk[..read]);
        }
        assert_eq!(observed, expected);
        assert!(exact.is_verified());
    }

    let empty_root = TempRoot::new("reader-empty");
    let empty_store = store(empty_root.path(), ArtifactConfig::default());
    let sealed = empty_store
        .start_job(1, OutputTargets::default())
        .unwrap()
        .seal(JobState::Exited)
        .unwrap();
    let mut empty = open_stream_reader(empty_root.path(), &sealed.record.stdout).unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(empty.read_chunk(&mut byte).unwrap(), 0);
    assert!(empty.is_verified());
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
