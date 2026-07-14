use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_schema::DataType;
use cowshed_core::api::{
    AdmissionCommitment, BinaryData, CONTROLLER_COMMITMENT_VERSION, CheckpointCommitment,
    CommandArg, ControllerCommitment, DtoError, ForkCommitment, JobId, JobState,
    MAX_COMMAND_ARG_BYTES, MAX_INLINE_OUTPUT_BYTES, OutputLimitInfo, OutputPublication,
    OutputStorage, ProtectedOutput, PublicationPolicy, RestoreCommitment, Sha256Digest, StreamInfo,
    TerminalCommitment, WorkspaceIncarnation, WorkspaceIntroducedCommitment, WorkspacePath,
    WorkspaceRetiredCommitment,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::job_artifact::{
    ArtifactConfig, ArtifactError, ArtifactStore, CommitmentPriorContext, JobArtifactToken,
    OutputTargets, ProtectedRecord, PublicationStage, StreamKind, StreamTarget,
    controller_commitment_schema, controller_commitments_from_batch,
    controller_commitments_to_batch, open_stream_reader, protected_record_schema, read_stream,
    reconcile_commitments, recover_records, recover_records_with_budget, validate_commitments,
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
fn begin_with_argv(
    store: &mut ArtifactStore,
    grant_revision: u64,
    argv: &[CommandArg],
    targets: OutputTargets,
) -> JobArtifactToken {
    let job_id = store.next_job_id().unwrap();
    store
        .begin_job(job_id, grant_revision, argv, targets)
        .unwrap()
}

fn begin(
    store: &mut ArtifactStore,
    grant_revision: u64,
    targets: OutputTargets,
) -> JobArtifactToken {
    begin_with_argv(store, grant_revision, &["true".into()], targets)
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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 4,
            supervisor_buffer_budget_bytes: 64,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(&mut store, 7, OutputTargets::default());
    store.append(&token, StreamKind::Stdout, b"four").unwrap();
    store.append(&token, StreamKind::Stderr, b"spill").unwrap();
    let sealed = store.finish(token, JobState::Exited).unwrap();

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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 10,
            supervisor_buffer_budget_bytes: 4,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(&mut store, 1, OutputTargets::default());
    store.append(&token, StreamKind::Stdout, b"1234").unwrap();
    assert_eq!(store.buffered_bytes(), 4);
    store.append(&token, StreamKind::Stderr, b"x").unwrap();
    assert_eq!(store.buffered_bytes(), 4);
    let sealed = store.finish(token, JobState::Exited).unwrap();
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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 64,
            supervisor_buffer_budget_bytes: 16,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let mut tokens = Vec::new();
    for _ in 0..64 {
        let token = begin(&mut store, 1, OutputTargets::default());
        store.append(&token, StreamKind::Stdout, b"x").unwrap();
        assert!(store.buffered_bytes() <= 16);
        tokens.push(token);
    }
    for token in tokens {
        store.abort(token).unwrap();
    }
    assert_eq!(store.buffered_bytes(), 0);
}

#[test]
fn stale_and_foreign_tokens_are_typed_conflicts() {
    let root = TempRoot::new("stale-token");
    let mut actor_store = store(root.path(), ArtifactConfig::default());
    let stale_id = actor_store.next_job_id().unwrap();
    let stale = actor_store
        .begin_job(stale_id, 1, &["true".into()], OutputTargets::default())
        .unwrap();
    actor_store.abort(stale).unwrap();
    assert!(matches!(
        actor_store.begin_job(stale_id, 1, &["true".into()], OutputTargets::default()),
        Err(ArtifactError::TokenConflict { .. })
    ));

    let foreign_root = TempRoot::new("foreign-token");
    let mut foreign_store = store(foreign_root.path(), ArtifactConfig::default());
    let foreign = begin(&mut foreign_store, 1, OutputTargets::default());
    assert!(matches!(
        actor_store.append(&foreign, StreamKind::Stdout, b"foreign"),
        Err(ArtifactError::TokenConflict { message, .. })
            if message.contains("another artifact store")
    ));
    foreign_store.abort(foreign).unwrap();
}

#[test]
fn combined_quota_accepts_exact_boundary_and_reports_first_crossing() {
    let exact_root = TempRoot::new("quota-exact");
    let mut exact = store(
        exact_root.path(),
        ArtifactConfig {
            inline_cap_bytes: 16,
            supervisor_buffer_budget_bytes: 16,
            combined_output_quota_bytes: 5,
            ..ArtifactConfig::default()
        },
    );
    let exact_token = begin(&mut exact, 1, OutputTargets::default());
    exact
        .append(&exact_token, StreamKind::Stdout, b"123")
        .unwrap();
    exact
        .append(&exact_token, StreamKind::Stderr, b"45")
        .unwrap();
    assert!(exact.output_limit(&exact_token).unwrap().is_none());
    let exact_sealed = exact.finish(exact_token, JobState::Exited).unwrap();
    assert_eq!(
        exact_sealed.record.stdout.bytes + exact_sealed.record.stderr.bytes,
        5
    );

    let crossed_root = TempRoot::new("quota-crossed");
    let mut crossed = store(
        crossed_root.path(),
        ArtifactConfig {
            inline_cap_bytes: 16,
            supervisor_buffer_budget_bytes: 16,
            combined_output_quota_bytes: 5,
            ..ArtifactConfig::default()
        },
    );
    let crossed_token = begin(&mut crossed, 1, OutputTargets::default());
    crossed
        .append(&crossed_token, StreamKind::Stdout, b"123")
        .unwrap();
    let crossing = crossed
        .append(&crossed_token, StreamKind::Stderr, b"456")
        .unwrap();
    assert_eq!(crossing.accepted_bytes, 2);
    assert_eq!(crossing.output_limit.as_ref().unwrap().limit_bytes, 5);
    assert_eq!(crossing.output_limit.as_ref().unwrap().crossing_bytes, 6);
    assert_eq!(
        crossed
            .output_limit(&crossed_token)
            .unwrap()
            .unwrap()
            .crossing_bytes,
        6
    );
    let rejected = crossed
        .append(&crossed_token, StreamKind::Stdout, b"ignored")
        .unwrap();
    assert_eq!(rejected.accepted_bytes, 0);
    assert_eq!(rejected.output_limit, crossing.output_limit);
    let sealed = crossed
        .finish(crossed_token, JobState::OutputLimit)
        .unwrap();
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
fn unsafe_argv_rejects_before_artifact_creation_or_id_advance() {
    let root = TempRoot::new("unsafe-argv");
    let mut store = store(root.path(), ArtifactConfig::default());
    let job_id = store.next_job_id().unwrap();
    let records = root.path().join(".cowshed/job/records.arrow");

    let nul = [CommandArg::from(OsString::from_vec(vec![b'x', 0]))];
    assert!(matches!(
        store.begin_job(job_id, 1, &nul, OutputTargets::default()),
        Err(ArtifactError::Dto(DtoError::CommandArgumentContainsNul))
    ));
    assert_eq!(store.next_job_id().unwrap(), job_id);
    assert!(!records.exists());

    let oversize = [CommandArg::from(OsString::from_vec(vec![
        b'x';
        MAX_COMMAND_ARG_BYTES
            + 1
    ]))];
    assert!(matches!(
        store.begin_job(job_id, 1, &oversize, OutputTargets::default()),
        Err(ArtifactError::Dto(DtoError::CommandArgumentTooLarge))
    ));
    assert_eq!(store.next_job_id().unwrap(), job_id);
    assert!(!records.exists());
}

#[test]
fn non_utf8_argv_round_trips_through_binary_arrow_and_recovery() {
    let root = TempRoot::new("argv-binary");
    let mut store = store(root.path(), ArtifactConfig::default());
    let raw = vec![0xff, b'a', 0x80];
    let argv = vec![
        CommandArg::from(OsString::from_vec(raw.clone())),
        CommandArg::from("--flag"),
    ];
    let token = begin_with_argv(&mut store, 7, &argv, OutputTargets::default());
    let sealed = store.finish(token, JobState::Exited).unwrap();
    assert_eq!(sealed.record.argv, argv);

    let schema = protected_record_schema();
    let field = schema.field_with_name("argv").unwrap();
    let DataType::List(item) = field.data_type() else {
        panic!("argv must be an Arrow list");
    };
    assert_eq!(item.data_type(), &DataType::Binary);

    let recovery = recover_records(&root.path().join(".cowshed/job/records.arrow")).unwrap();
    let recovered = recovery
        .frames
        .iter()
        .filter_map(|frame| match &frame.record {
            ProtectedRecord::Job(record) if record.state == JobState::Exited => Some(record),
            _ => None,
        })
        .next()
        .expect("terminal job record");
    assert_eq!(recovered.argv, argv);
    assert_eq!(recovered.argv[0].as_os_str().as_bytes(), raw);
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
    let mut store = store(root.path(), ArtifactConfig::default());
    let token = begin(&mut store, 1, OutputTargets::default());
    store.append(&token, StreamKind::Stdout, &bytes).unwrap();
    let sealed = store.finish(token, JobState::Exited).unwrap();
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
    let mut first = store(root.path(), ArtifactConfig::default());
    let first_token = begin(&mut first, 1, OutputTargets::default());
    assert_eq!(first_token.job_id().get(), 1);
    first.finish(first_token, JobState::Exited).unwrap();
    assert!(!root.path().join(".cowshed/job/1").exists());
    drop(first);

    let mut restarted = store(root.path(), ArtifactConfig::default());
    assert_eq!(restarted.recovery().next_job_id.get(), 2);
    let second = begin(&mut restarted, 1, OutputTargets::default());
    assert_eq!(second.job_id().get(), 2);
    restarted.abort(second).unwrap();
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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 2,
            supervisor_buffer_budget_bytes: 2,
            combined_output_quota_bytes: 100,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(
        &mut store,
        1,
        OutputTargets {
            stdout: StreamTarget::Redirect {
                source: source.clone(),
                descriptor,
            },
            stderr: StreamTarget::Captured,
        },
    );
    assert_ne!(
        unsafe { libc::fcntl(redirect_fd, libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
    store
        .append(&token, StreamKind::Stdout, b"protected")
        .unwrap();
    fs::write(root.path().join(source.as_path()), b"mutated before seal").unwrap();
    let sealed = store.finish(token, JobState::Exited).unwrap();
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
    let mut store = store(root.path(), ArtifactConfig::default());
    let token = begin(&mut store, 1, OutputTargets::default());
    store.finish(token, JobState::Exited).unwrap();
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
    let mut store = store(root.path(), ArtifactConfig::default());
    let token = begin(&mut store, 7, OutputTargets::default());
    store
        .append(&token, StreamKind::Stdout, b"authority")
        .unwrap();
    let sealed = store.finish(token, JobState::Exited).unwrap();
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
    let mut store = store(root.path(), ArtifactConfig::default());
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
fn workspace_lifecycle_replay_preserves_retired_jobs_and_rejects_stale_order() {
    let repo = RepoId::parse("acme/widget").unwrap();
    let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
    let empty = Sha256Digest::compute(&[]);
    let history = vec![
        ControllerCommitment::WorkspaceIntroduced(WorkspaceIntroducedCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 1,
            repo_id: repo.clone(),
            workspace_incarnation: incarnation.clone(),
        }),
        ControllerCommitment::Admission(AdmissionCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 2,
            repo_id: repo.clone(),
            workspace_incarnation: incarnation.clone(),
            job_id: JobId::new(1).unwrap(),
            grant_revision: 7,
        }),
        ControllerCommitment::Terminal(TerminalCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 3,
            repo_id: repo.clone(),
            workspace_incarnation: incarnation.clone(),
            job_id: JobId::new(1).unwrap(),
            state: JobState::Exited,
            grant_revision: 7,
            stdout_bytes: 0,
            stdout_sha256: empty,
            stderr_bytes: 0,
            stderr_sha256: empty,
            batch_sha256: empty,
            output_limit: None,
        }),
        ControllerCommitment::WorkspaceRetired(WorkspaceRetiredCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 4,
            repo_id: repo.clone(),
            workspace_incarnation: incarnation.clone(),
        }),
    ];
    let baseline = CommitmentPriorContext::new(repo.clone(), []);
    assert_eq!(
        validate_commitments(&baseline, &history)
            .unwrap()
            .last_order(),
        4
    );

    let batch = controller_commitments_to_batch(&history).unwrap();
    assert_eq!(
        controller_commitments_from_batch(&batch, &baseline).unwrap(),
        history
    );
    for row in [0, 3] {
        assert!(!batch.column(4).is_null(row));
        for column in 5..batch.num_columns() {
            assert!(
                batch.column(column).is_null(row),
                "unexpected lifecycle value in {}",
                batch.schema().field(column).name()
            );
        }
    }
    assert_eq!(
        serde_json::to_value(&history[0]).unwrap(),
        serde_json::json!({
            "kind": "workspaceIntroduced",
            "version": 1,
            "order": 1,
            "repoId": "acme/widget",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80"
        })
    );
    assert_eq!(
        serde_json::to_value(&history[3]).unwrap(),
        serde_json::json!({
            "kind": "workspaceRetired",
            "version": 1,
            "order": 4,
            "repoId": "acme/widget",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80"
        })
    );

    let introduced = history[0].clone();
    let retired = history[3].clone();
    let admission = history[1].clone();
    let terminal = history[2].clone();
    let cases = [
        vec![with_order(admission.clone(), 1)],
        vec![
            introduced.clone(),
            with_order(retired.clone(), 2),
            with_order(admission, 3),
        ],
        vec![introduced.clone(), with_order(introduced.clone(), 2)],
        vec![
            introduced.clone(),
            with_order(retired.clone(), 2),
            with_order(retired.clone(), 3),
        ],
        vec![with_order(retired.clone(), 1)],
        vec![
            introduced.clone(),
            with_order(retired.clone(), 2),
            with_order(introduced, 3),
        ],
        vec![
            history[0].clone(),
            history[1].clone(),
            with_order(retired, 3),
            with_order(terminal, 4),
        ],
    ];
    for values in cases {
        assert!(matches!(
            validate_commitments(&baseline, &values),
            Err(ArtifactError::Integrity { .. })
        ));
    }
}

fn with_order(mut commitment: ControllerCommitment, order: u64) -> ControllerCommitment {
    match &mut commitment {
        ControllerCommitment::WorkspaceIntroduced(value) => value.order = order,
        ControllerCommitment::WorkspaceRetired(value) => value.order = order,
        ControllerCommitment::Admission(value) => value.order = order,
        ControllerCommitment::Terminal(value) => value.order = order,
        ControllerCommitment::Checkpoint(value) => value.order = order,
        ControllerCommitment::Fork(value) => value.order = order,
        ControllerCommitment::Restore(value) => value.order = order,
    }
    commitment
}

#[test]
fn retained_checkpoint_can_restore_repeatedly_only_to_fresh_destinations() {
    let repo = RepoId::parse("acme/widget").unwrap();
    let source = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
    let forked = WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").unwrap();
    let first_restore = WorkspaceIncarnation::new("2198f2c0b7e34dc795f17b238b331c80").unwrap();
    let second_restore = WorkspaceIncarnation::new("3198f2c0b7e34dc795f17b238b331c80").unwrap();
    let baseline = CommitmentPriorContext::new(repo.clone(), []);
    let history = vec![
        ControllerCommitment::WorkspaceIntroduced(WorkspaceIntroducedCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 1,
            repo_id: repo.clone(),
            workspace_incarnation: source.clone(),
        }),
        ControllerCommitment::Checkpoint(CheckpointCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 2,
            repo_id: repo.clone(),
            origin_incarnation: source.clone(),
            checkpoint_id: "checkpoint-1".into(),
            barrier_id: 1,
            manifest_batch_sha256: Sha256Digest::compute(b"manifest"),
        }),
        ControllerCommitment::Fork(ForkCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 3,
            repo_id: repo.clone(),
            source_incarnation: source.clone(),
            destination_incarnation: forked.clone(),
        }),
        ControllerCommitment::Restore(RestoreCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 4,
            repo_id: repo.clone(),
            source_checkpoint: "checkpoint-1".into(),
            source_incarnation: source.clone(),
            destination_incarnation: first_restore.clone(),
        }),
        ControllerCommitment::Restore(RestoreCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 5,
            repo_id: repo.clone(),
            source_checkpoint: "checkpoint-1".into(),
            source_incarnation: source.clone(),
            destination_incarnation: second_restore.clone(),
        }),
        ControllerCommitment::Admission(AdmissionCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 6,
            repo_id: repo.clone(),
            workspace_incarnation: second_restore,
            job_id: JobId::new(1).unwrap(),
            grant_revision: 1,
        }),
    ];
    validate_commitments(&baseline, &history).unwrap();

    let mut reused_destination = history.clone();
    reused_destination.push(ControllerCommitment::Restore(RestoreCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order: 7,
        repo_id: repo.clone(),
        source_checkpoint: "checkpoint-1".into(),
        source_incarnation: source.clone(),
        destination_incarnation: first_restore,
    }));
    assert!(matches!(
        validate_commitments(&baseline, &reused_destination),
        Err(ArtifactError::Integrity { .. })
    ));

    let mut unknown_checkpoint = history[..2].to_vec();
    unknown_checkpoint.push(ControllerCommitment::Restore(RestoreCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order: 3,
        repo_id: repo.clone(),
        source_checkpoint: "unknown".into(),
        source_incarnation: source.clone(),
        destination_incarnation: WorkspaceIncarnation::new("4198f2c0b7e34dc795f17b238b331c80")
            .unwrap(),
    }));
    assert!(matches!(
        validate_commitments(&baseline, &unknown_checkpoint),
        Err(ArtifactError::Integrity { .. })
    ));

    let mut mismatched_origin = history[..2].to_vec();
    mismatched_origin.push(ControllerCommitment::Restore(RestoreCommitment {
        version: CONTROLLER_COMMITMENT_VERSION,
        order: 3,
        repo_id: repo,
        source_checkpoint: "checkpoint-1".into(),
        source_incarnation: forked,
        destination_incarnation: WorkspaceIncarnation::new("5198f2c0b7e34dc795f17b238b331c80")
            .unwrap(),
    }));
    assert!(matches!(
        validate_commitments(&baseline, &mismatched_origin),
        Err(ArtifactError::Integrity { .. })
    ));
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
    let mut initial_store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 4,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(&mut initial_store, 1, OutputTargets::default());
    initial_store
        .append(&token, StreamKind::Stdout, b"sealed output")
        .unwrap();
    let sealed = initial_store.finish(token, JobState::Exited).unwrap();
    let protected = match sealed.record.stdout.storage.artifact() {
        ProtectedOutput::File { path } => root.path().join(path.as_path()),
        ProtectedOutput::Inline { .. } => panic!("expected promoted protected output"),
    };
    drop(initial_store);
    let mut store = store(
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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(&mut store, 1, OutputTargets::default());
    store
        .append(&token, StreamKind::Stdout, b"authority")
        .unwrap();
    let sealed = store.finish(token, JobState::Exited).unwrap();

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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 64,
            supervisor_buffer_budget_bytes: 128,
            ..ArtifactConfig::default()
        },
    );
    let committed_token = begin(&mut store, 1, OutputTargets::default());
    let committed = store.finish(committed_token, JobState::Exited).unwrap();
    let active_token = begin(&mut store, 2, OutputTargets::default());
    store
        .append(&active_token, StreamKind::Stdout, b"background-prefix")
        .unwrap();
    store.prepare_background(&active_token).unwrap();
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
        .find(|job| job.job_id == active_token.job_id())
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

    store.abort(active_token).unwrap();
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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(&mut store, 1, OutputTargets::default());
    store
        .append(&token, StreamKind::Stdout, b"stdout-authority")
        .unwrap();
    store.append(&token, StreamKind::Stderr, b"err").unwrap();
    store.finish(token, JobState::Exited).unwrap();
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
        let mut store = store(
            root.path(),
            ArtifactConfig {
                inline_cap_bytes: 1,
                ..ArtifactConfig::default()
            },
        );
        let expected = b"sealed-authority";
        let token = begin(&mut store, 1, OutputTargets::default());
        store.append(&token, StreamKind::Stdout, expected).unwrap();
        let sealed = store.finish(token, JobState::Exited).unwrap();
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
        if matches!(mutation, Mutation::WritableMode) {
            assert!(matches!(
                ArtifactStore::open(
                    root.path(),
                    RepoId::parse("acme/widget").unwrap(),
                    WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
                    ArtifactConfig::default(),
                ),
                Err(ArtifactError::Integrity { message, .. })
                    if message.contains("not sealed")
            ));
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
    let mut store = store(
        &actual,
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(&mut store, 1, OutputTargets::default());
    store
        .append(&token, StreamKind::Stdout, b"protected")
        .unwrap();
    let sealed = store.finish(token, JobState::Exited).unwrap();
    let alias = container.path().join("alias");
    symlink(&actual, &alias).unwrap();

    assert!(matches!(
        read_stream(&alias, &sealed.record.stdout),
        Err(ArtifactError::Io { .. }) | Err(ArtifactError::Integrity { .. })
    ));

    let protected = actual.join(".cowshed");
    let backing = actual.join(".cowshed-real");
    fs::rename(&protected, &backing).unwrap();
    symlink(".cowshed-real", &protected).unwrap();
    assert!(matches!(
        read_stream(&actual, &sealed.record.stdout),
        Err(ArtifactError::Io { .. }) | Err(ArtifactError::Integrity { .. })
    ));
}

#[test]
fn streaming_recovery_enforces_retained_budget_before_payload_allocation() {
    let root = TempRoot::new("recovery-budget");
    let mut store = store(root.path(), ArtifactConfig::default());
    let _token = begin(&mut store, 1, OutputTargets::default());
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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let token = begin(&mut store, 5, OutputTargets::default());
    store.append(&token, StreamKind::Stdout, b"stdout").unwrap();
    store.append(&token, StreamKind::Stderr, b"stderr").unwrap();
    let completed = store
        .finish_and_publish(
            token,
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
    let mut store = store(
        root.path(),
        ArtifactConfig {
            inline_cap_bytes: 1,
            ..ArtifactConfig::default()
        },
    );
    let bytes = vec![0x5a; MAX_INLINE_OUTPUT_BYTES + 1];
    let token = begin(&mut store, 1, OutputTargets::default());
    store.append(&token, StreamKind::Stdout, &bytes).unwrap();
    let sealed = store.finish(token, JobState::Exited).unwrap();
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
        let mut store = store(
            root.path(),
            ArtifactConfig {
                inline_cap_bytes,
                ..ArtifactConfig::default()
            },
        );
        let expected = b"abcdef";
        let token = begin(&mut store, 1, OutputTargets::default());
        store.append(&token, StreamKind::Stdout, expected).unwrap();
        let sealed = store.finish(token, JobState::Exited).unwrap();

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
    let mut empty_store = store(empty_root.path(), ArtifactConfig::default());
    let empty_token = begin(&mut empty_store, 1, OutputTargets::default());
    let sealed = empty_store.finish(empty_token, JobState::Exited).unwrap();
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
