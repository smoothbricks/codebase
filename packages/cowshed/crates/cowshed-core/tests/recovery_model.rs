use std::collections::BTreeSet;

use cowshed_core::metadata::WorkspaceIncarnation;
use cowshed_core::storage::recovery::{
    AuthoritativeObservations, BeginOutcome, CheckedLifecyclePlan, CheckpointMetadata,
    ExecutionOutcome, Failpoint, FormatMetadata, GcPass, Generation, MetadataGeneration,
    ObjectNamespace, ProtocolEvent, RecoveryDisposition, RecoveryError, RecoveryModel,
    RetentionMetadata, StaleDimension, StoredObject, TopologyMetadata, TransactionKind,
    TransactionPhase, TransactionSpec, enumerate_published,
};
use proptest::prelude::*;

const ALL_KINDS: [TransactionKind; 4] = [
    TransactionKind::Adopt,
    TransactionKind::Create,
    TransactionKind::Restore,
    TransactionKind::Retire,
];

const ALL_PHASES: [TransactionPhase; 14] = [
    TransactionPhase::Prepare,
    TransactionPhase::Prepared,
    TransactionPhase::Validated,
    TransactionPhase::IncarnationMinted,
    TransactionPhase::TokenMinted,
    TransactionPhase::StagedAuthoritySynced,
    TransactionPhase::CanonicalSwapped,
    TransactionPhase::CanonicalValidated,
    TransactionPhase::DetachedMetadataReplaced,
    TransactionPhase::Published,
    TransactionPhase::Admitted,
    TransactionPhase::CleanupPending,
    TransactionPhase::Complete,
    TransactionPhase::RolledBack,
];

const STALE_DIMENSIONS: [StaleDimension; 6] = [
    StaleDimension::Incarnation,
    StaleDimension::GrantRevision,
    StaleDimension::Checkpoint,
    StaleDimension::Format,
    StaleDimension::Topology,
    StaleDimension::Retirement,
];

fn incarnation(digit: char) -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(digit.to_string().repeat(32)).unwrap()
}

fn observations(kind: TransactionKind) -> AuthoritativeObservations {
    AuthoritativeObservations {
        incarnation: matches!(kind, TransactionKind::Restore | TransactionKind::Retire)
            .then(|| incarnation('a')),
        grant_revision: 17,
        checkpoint: Some(CheckpointMetadata { revision: 23 }),
        format: FormatMetadata { version: 3 },
        topology: TopologyMetadata { revision: 29 },
        retired: false,
        retention: RetentionMetadata {
            retain_until_revision: 101,
        },
    }
}

fn spec(kind: TransactionKind, suffix: &str) -> TransactionSpec {
    let next = (kind != TransactionKind::Retire).then(|| incarnation('b'));
    TransactionSpec::new(kind, "topic", format!("tx-{suffix}"), next).unwrap()
}

fn plan(kind: TransactionKind, suffix: &str) -> CheckedLifecyclePlan {
    CheckedLifecyclePlan::new(spec(kind, suffix), observations(kind)).unwrap()
}

fn initial(kind: TransactionKind) -> RecoveryModel {
    match RecoveryModel::begin(plan(kind, "fixture"), observations(kind)) {
        BeginOutcome::Started(model) => *model,
        BeginOutcome::Conflict(conflict) => panic!("fresh plan conflicted: {conflict:?}"),
    }
}

fn interrupted(kind: TransactionKind, phase: TransactionPhase) -> RecoveryModel {
    match Failpoint::after(phase)
        .interrupt(plan(kind, "failpoint"), observations(kind))
        .unwrap()
    {
        ExecutionOutcome::Interrupted(model) => *model,
        ExecutionOutcome::Conflict(conflict) => panic!("fresh plan conflicted: {conflict:?}"),
    }
}

fn advance_to(mut model: RecoveryModel, target: TransactionPhase) -> RecoveryModel {
    while model.phase() != target {
        model = model.advance().unwrap();
    }
    model
}

fn complete(mut model: RecoveryModel) -> RecoveryModel {
    while model.phase() != TransactionPhase::Complete {
        model = model.advance().unwrap();
    }
    model
}

fn target(kind: TransactionKind) -> MetadataGeneration {
    match kind {
        TransactionKind::Adopt | TransactionKind::Create | TransactionKind::Restore => {
            MetadataGeneration::New
        }
        TransactionKind::Retire => MetadataGeneration::Absent,
    }
}

fn old(kind: TransactionKind) -> MetadataGeneration {
    match kind {
        TransactionKind::Adopt | TransactionKind::Create => MetadataGeneration::Absent,
        TransactionKind::Restore | TransactionKind::Retire => MetadataGeneration::Old,
    }
}

fn expected_old_authority(kind: TransactionKind) -> Option<Generation> {
    matches!(kind, TransactionKind::Restore | TransactionKind::Retire).then_some(Generation::Old)
}

fn expected_new_authority(kind: TransactionKind) -> Option<Generation> {
    (kind != TransactionKind::Retire).then_some(Generation::New)
}

fn event_position(events: &[ProtocolEvent], expected: &ProtocolEvent) -> usize {
    events.iter().position(|event| event == expected).unwrap()
}

fn make_stale(
    mut observed: AuthoritativeObservations,
    dimension: StaleDimension,
) -> AuthoritativeObservations {
    match dimension {
        StaleDimension::Incarnation => observed.incarnation = Some(incarnation('c')),
        StaleDimension::GrantRevision => observed.grant_revision += 1,
        StaleDimension::Checkpoint => {
            observed.checkpoint = Some(CheckpointMetadata { revision: 24 });
        }
        StaleDimension::Format => observed.format.version += 1,
        StaleDimension::Topology => observed.topology.revision += 1,
        StaleDimension::Retirement => observed.retired = !observed.retired,
    }
    observed
}

#[test]
fn transaction_specs_are_capability_free_and_require_the_incarnation_shape() {
    assert_eq!(
        TransactionSpec::new(TransactionKind::Create, "ws", "tx", None),
        Err(RecoveryError::MissingNextIncarnation(
            TransactionKind::Create
        ))
    );
    assert_eq!(
        TransactionSpec::new(TransactionKind::Retire, "ws", "tx", Some(incarnation('b')),),
        Err(RecoveryError::UnexpectedNextIncarnation(
            TransactionKind::Retire
        ))
    );
    assert_eq!(
        TransactionSpec::new(TransactionKind::Restore, "", "tx", Some(incarnation('b'))),
        Err(RecoveryError::EmptyLogicalName)
    );
    assert_eq!(
        TransactionSpec::new(TransactionKind::Restore, "ws", "", Some(incarnation('b'))),
        Err(RecoveryError::EmptyTransactionId)
    );

    let restore = spec(TransactionKind::Restore, "shape");
    let mut absent = observations(TransactionKind::Restore);
    absent.incarnation = None;
    assert_eq!(
        CheckedLifecyclePlan::new(restore.clone(), absent),
        Err(RecoveryError::MissingCurrentIncarnation(
            TransactionKind::Restore
        ))
    );
    let mut retired = observations(TransactionKind::Restore);
    retired.retired = true;
    assert_eq!(
        CheckedLifecyclePlan::new(restore.clone(), retired),
        Err(RecoveryError::AlreadyRetired(TransactionKind::Restore))
    );
    let mut without_checkpoint = observations(TransactionKind::Restore);
    without_checkpoint.checkpoint = None;
    assert_eq!(
        CheckedLifecyclePlan::new(restore.clone(), without_checkpoint),
        Err(RecoveryError::MissingRestoreCheckpoint)
    );
    let mut reused = observations(TransactionKind::Restore);
    reused.incarnation = restore.next_incarnation().cloned();
    assert_eq!(
        CheckedLifecyclePlan::new(restore.clone(), reused),
        Err(RecoveryError::ReusedIncarnation)
    );
}

#[test]
fn prepare_stages_only_replacements_and_non_retire_completion_skips_cleanup() {
    for kind in [
        TransactionKind::Adopt,
        TransactionKind::Create,
        TransactionKind::Restore,
    ] {
        let prepared = initial(kind).advance().unwrap();
        assert_eq!(prepared.phase(), TransactionPhase::Prepared);
        assert!(prepared.staging_present());

        let admitted = advance_to(prepared, TransactionPhase::Admitted);
        let complete = admitted.advance().unwrap();
        assert_eq!(complete.phase(), TransactionPhase::Complete);
        assert!(!complete.cleanup_present());
        assert!(
            !complete
                .events()
                .iter()
                .any(|event| event == &ProtocolEvent::CleanupDeferred)
        );
    }

    let prepared = initial(TransactionKind::Retire).advance().unwrap();
    assert_eq!(prepared.phase(), TransactionPhase::Prepared);
    assert!(!prepared.staging_present());
}

#[test]
fn capability_is_absent_from_plan_and_every_record_before_token_minted() {
    let checked = plan(TransactionKind::Restore, "capability");
    let durable_input = format!("{checked:?}");
    assert!(!durable_input.contains("gateway-"));

    for phase in [
        TransactionPhase::Prepare,
        TransactionPhase::Prepared,
        TransactionPhase::Validated,
        TransactionPhase::IncarnationMinted,
    ] {
        let record = interrupted(TransactionKind::Restore, phase);
        assert_eq!(record.minted_authority(), None, "phase {phase:?}");
        assert!(
            !format!("{record:?}").contains("gateway-"),
            "phase {phase:?}"
        );
    }

    let token_minted = interrupted(TransactionKind::Restore, TransactionPhase::TokenMinted);
    let authority = token_minted.minted_authority().unwrap();
    assert_eq!(authority.incarnation(), &incarnation('b'));
    assert!(authority.token().starts_with("gateway-tx-failpoint-"));
}

#[test]
fn spec_and_object_accessors_distinguish_checkpoint_retention_from_trash() {
    let record = spec(TransactionKind::Restore, "fields");
    assert_eq!(record.kind(), TransactionKind::Restore);
    assert_eq!(record.logical_name(), "topic");
    assert_eq!(record.transaction_id(), "tx-fields");
    assert_eq!(record.next_incarnation().unwrap(), &incarnation('b'));

    let staging = record.staging_object().unwrap();
    assert_eq!(staging.namespace(), ObjectNamespace::Staging);
    assert_eq!(staging.name(), ".staging/topic-tx-fields");
    assert_eq!(record.cleanup_object(), None);
    let retained = record.restore_checkpoint_object().unwrap();
    assert_eq!(retained.namespace(), ObjectNamespace::Checkpoint);
    assert_eq!(retained.name(), ".checkpoints/topic-pre-restore-tx-fields");

    let retire = spec(TransactionKind::Retire, "retire");
    assert_eq!(retire.staging_object(), None);
    assert_eq!(retire.restore_checkpoint_object(), None);
    assert_eq!(
        retire.cleanup_object().unwrap().namespace(),
        ObjectNamespace::Trash
    );
}

#[test]
fn every_stale_authority_dimension_returns_a_structured_zero_effect_conflict() {
    for dimension in STALE_DIMENSIONS {
        let expected = observations(TransactionKind::Restore);
        let conflict = match RecoveryModel::begin(
            CheckedLifecyclePlan::new(spec(TransactionKind::Restore, "stale"), expected.clone())
                .unwrap(),
            make_stale(expected, dimension),
        ) {
            BeginOutcome::Conflict(conflict) => conflict,
            BeginOutcome::Started(_) => panic!("{dimension:?} mismatch started execution"),
        };
        assert_eq!(conflict.stale_dimensions(), &[dimension]);
        assert_eq!(conflict.effect_count(), 0);
        assert_eq!(conflict.phase(), None);
    }
}

#[test]
fn execute_revalidates_all_dimensions_before_any_failpoint_or_prepare_effect() {
    let expected = observations(TransactionKind::Restore);
    let mut observed = expected.clone();
    for dimension in STALE_DIMENSIONS {
        observed = make_stale(observed, dimension);
    }
    for failpoint in ALL_PHASES {
        let outcome = Failpoint::after(failpoint)
            .interrupt(
                CheckedLifecyclePlan::new(
                    spec(TransactionKind::Restore, "all-stale"),
                    expected.clone(),
                )
                .unwrap(),
                observed.clone(),
            )
            .unwrap();
        let ExecutionOutcome::Conflict(conflict) = outcome else {
            panic!("stale execution reached failpoint {failpoint:?}");
        };
        assert_eq!(conflict.stale_dimensions(), STALE_DIMENSIONS.as_slice());
        assert_eq!(conflict.effect_count(), 0);
        assert_eq!(conflict.phase(), None);
    }
}

#[test]
fn all_failpoints_are_deterministically_reachable_or_rejected_for_each_kind() {
    for kind in ALL_KINDS {
        let mut model = initial(kind);
        let mut reachable = Vec::new();
        loop {
            reachable.push(model.phase());
            if model.phase() == TransactionPhase::Complete {
                break;
            }
            model = model.advance().unwrap();
        }

        for phase in ALL_PHASES {
            let result = Failpoint::after(phase).interrupt(plan(kind, "table"), observations(kind));
            if reachable.contains(&phase) {
                let ExecutionOutcome::Interrupted(record) = result.unwrap() else {
                    panic!("fresh {kind:?} plan conflicted at {phase:?}");
                };
                assert_eq!(record.phase(), phase);
            } else {
                assert_eq!(
                    result,
                    Err(RecoveryError::UnreachableFailpoint { kind, phase })
                );
            }
        }
    }
}

#[test]
fn restore_orders_identity_token_flush_publication_fsync_cutover_and_admission() {
    let completed = complete(initial(TransactionKind::Restore));
    assert_eq!(completed.phase(), TransactionPhase::Complete);
    assert_eq!(completed.canonical(), MetadataGeneration::New);
    assert_eq!(completed.metadata(), MetadataGeneration::New);
    assert_eq!(completed.synced_metadata(), MetadataGeneration::New);
    assert_eq!(completed.accepted_authority(), Some(Generation::New));
    assert_eq!(completed.admitted_authority(), Some(Generation::New));
    assert!(!completed.staging_present());
    assert!(!completed.cleanup_present());

    let events = completed.events();
    let incarnation = event_position(events, &ProtocolEvent::IncarnationMinted);
    let token = event_position(events, &ProtocolEvent::TokenMinted);
    let flush = event_position(events, &ProtocolEvent::StagedAuthorityFlushedAndVerified);
    let replace = event_position(events, &ProtocolEvent::AtomicMetadataReplace);
    let fsync = event_position(events, &ProtocolEvent::MetadataParentFsync);
    let cutover = event_position(
        events,
        &ProtocolEvent::AuthorityCutover {
            from: Some(Generation::Old),
            to: Some(Generation::New),
        },
    );
    let admission = event_position(events, &ProtocolEvent::NewAdmission);
    assert!(incarnation < token);
    assert!(token < flush);
    assert!(flush < replace);
    assert!(replace < fsync);
    assert!(fsync < cutover);
    assert!(cutover < admission);
}

#[test]
fn restore_retains_displaced_generation_as_undo_with_original_metadata() {
    let expected = observations(TransactionKind::Restore);
    let mut observed = expected.clone();
    observed.retention = RetentionMetadata {
        retain_until_revision: 777,
    };
    let model = match RecoveryModel::begin(
        CheckedLifecyclePlan::new(spec(TransactionKind::Restore, "undo"), expected).unwrap(),
        observed.clone(),
    ) {
        BeginOutcome::Started(model) => *model,
        BeginOutcome::Conflict(conflict) => {
            panic!("retention-only change conflicted: {conflict:?}")
        }
    };
    let swapped = advance_to(model, TransactionPhase::CanonicalSwapped);
    assert!(!swapped.cleanup_present());
    let retained = swapped.retained_checkpoint().unwrap();
    assert_eq!(retained.object().namespace(), ObjectNamespace::Checkpoint);
    assert_eq!(retained.displaced_incarnation(), &incarnation('a'));
    assert_eq!(retained.source_checkpoint(), observed.checkpoint);
    assert_eq!(retained.format(), observed.format);
    assert_eq!(retained.retention(), observed.retention);

    let before_publication = swapped.clone().recover();
    assert_eq!(before_publication.phase(), TransactionPhase::RolledBack);
    assert_eq!(before_publication.retained_checkpoint(), None);

    let published = advance_to(swapped, TransactionPhase::Published);
    let recovered = published.recover();
    assert_eq!(recovered.phase(), TransactionPhase::Complete);
    assert!(recovered.retained_checkpoint().is_some());
    let (after_gc, report) = recovered.clone().gc_pass();
    assert_eq!(after_gc, recovered);
    assert_eq!(report, GcPass::default());
    assert!(after_gc.retained_checkpoint().is_some());
    assert!(
        !after_gc
            .events()
            .iter()
            .any(|event| event == &ProtocolEvent::CleanupReclaimed)
    );
}

#[test]
fn only_retire_uses_trash_and_reclaim_cleanup() {
    for kind in [
        TransactionKind::Adopt,
        TransactionKind::Create,
        TransactionKind::Restore,
    ] {
        let completed = complete(initial(kind));
        assert!(!completed.cleanup_present());
        assert!(
            !completed
                .events()
                .iter()
                .any(|event| event == &ProtocolEvent::CleanupReclaimed)
        );
    }

    let cleanup = advance_to(
        initial(TransactionKind::Retire),
        TransactionPhase::CleanupPending,
    );
    assert!(cleanup.cleanup_present());
    assert_eq!(cleanup.retained_checkpoint(), None);
    let (collected, report) = cleanup.gc_pass();
    assert_eq!(
        report,
        GcPass {
            examined: 1,
            reclaimed: 1,
        }
    );
    assert_eq!(collected.phase(), TransactionPhase::Complete);
    assert!(!collected.cleanup_present());
    assert_eq!(
        collected.events().last(),
        Some(&ProtocolEvent::CleanupReclaimed)
    );
}

#[test]
fn endpoint_authority_is_exclusive_at_every_durable_phase() {
    for kind in ALL_KINDS {
        let mut model = initial(kind);
        loop {
            if let Some(admitted) = model.admitted_authority() {
                assert_eq!(model.accepted_authority(), Some(admitted));
            }
            if model.phase().recovery_disposition() == RecoveryDisposition::RollBack {
                assert_eq!(model.accepted_authority(), expected_old_authority(kind));
            } else if model.phase() != TransactionPhase::RolledBack {
                assert_eq!(model.accepted_authority(), expected_new_authority(kind));
            }
            if model.phase() == TransactionPhase::Complete {
                break;
            }
            model = model.advance().unwrap();
        }
    }
}

#[test]
fn every_phase_has_an_explicit_one_way_recovery_disposition() {
    for phase in [
        TransactionPhase::Prepare,
        TransactionPhase::Prepared,
        TransactionPhase::Validated,
        TransactionPhase::IncarnationMinted,
        TransactionPhase::TokenMinted,
        TransactionPhase::StagedAuthoritySynced,
        TransactionPhase::CanonicalSwapped,
        TransactionPhase::CanonicalValidated,
        TransactionPhase::DetachedMetadataReplaced,
    ] {
        assert_eq!(phase.recovery_disposition(), RecoveryDisposition::RollBack);
    }
    for phase in [
        TransactionPhase::Published,
        TransactionPhase::Admitted,
        TransactionPhase::CleanupPending,
    ] {
        assert_eq!(
            phase.recovery_disposition(),
            RecoveryDisposition::RollForward
        );
    }
    for phase in [TransactionPhase::Complete, TransactionPhase::RolledBack] {
        assert_eq!(phase.recovery_disposition(), RecoveryDisposition::Settled);
    }
}

#[test]
fn all_interrupted_records_converge_idempotently_without_losing_restore_undo() {
    for kind in ALL_KINDS {
        let mut interrupted = initial(kind);
        loop {
            let direction = interrupted.phase().recovery_disposition();
            let recovered_once = interrupted.clone().recover();
            let recovered_twice = recovered_once.clone().recover();
            assert_eq!(recovered_once, recovered_twice);
            assert!(!recovered_once.staging_present());

            match direction {
                RecoveryDisposition::RollBack => {
                    assert_eq!(recovered_once.phase(), TransactionPhase::RolledBack);
                    assert_eq!(recovered_once.canonical(), old(kind));
                    assert_eq!(recovered_once.metadata(), old(kind));
                    assert_eq!(recovered_once.synced_metadata(), old(kind));
                    assert_eq!(
                        recovered_once.accepted_authority(),
                        expected_old_authority(kind)
                    );
                    assert_eq!(
                        recovered_once.admitted_authority(),
                        expected_old_authority(kind)
                    );
                    assert!(!recovered_once.cleanup_present());
                    assert_eq!(recovered_once.retained_checkpoint(), None);
                }
                RecoveryDisposition::RollForward => {
                    assert_eq!(recovered_once.canonical(), target(kind));
                    assert_eq!(recovered_once.metadata(), target(kind));
                    assert_eq!(recovered_once.synced_metadata(), target(kind));
                    assert_eq!(
                        recovered_once.accepted_authority(),
                        expected_new_authority(kind)
                    );
                    assert_eq!(
                        recovered_once.admitted_authority(),
                        expected_new_authority(kind)
                    );
                    let expected_phase = if kind == TransactionKind::Retire {
                        TransactionPhase::CleanupPending
                    } else {
                        TransactionPhase::Complete
                    };
                    assert_eq!(recovered_once.phase(), expected_phase);
                    assert_eq!(
                        recovered_once.retained_checkpoint().is_some(),
                        kind == TransactionKind::Restore
                    );
                }
                RecoveryDisposition::Settled => assert_eq!(recovered_once, interrupted),
            }

            if interrupted.phase() == TransactionPhase::Complete {
                break;
            }
            interrupted = interrupted.advance().unwrap();
        }
    }
}

#[test]
fn metadata_replace_without_parent_fsync_is_still_before_the_fence() {
    let replaced = advance_to(
        initial(TransactionKind::Restore),
        TransactionPhase::DetachedMetadataReplaced,
    );
    assert_eq!(replaced.metadata(), MetadataGeneration::New);
    assert_eq!(replaced.synced_metadata(), MetadataGeneration::Old);
    assert_eq!(replaced.accepted_authority(), Some(Generation::Old));
    assert_eq!(replaced.admitted_authority(), None);

    let recovered = replaced.recover();
    assert_eq!(recovered.phase(), TransactionPhase::RolledBack);
    assert_eq!(recovered.metadata(), MetadataGeneration::Old);
    assert_eq!(recovered.synced_metadata(), MetadataGeneration::Old);
    assert_eq!(recovered.accepted_authority(), Some(Generation::Old));
    assert_eq!(recovered.admitted_authority(), Some(Generation::Old));
    assert_eq!(recovered.retained_checkpoint(), None);
}

#[test]
fn settled_transactions_refuse_further_execution() {
    let complete = complete(initial(TransactionKind::Create));
    let complete_phase = complete.phase();
    assert_eq!(
        complete.advance(),
        Err(RecoveryError::TransactionSettled(complete_phase))
    );

    let rolled_back = initial(TransactionKind::Adopt).recover();
    let rolled_back_phase = rolled_back.phase();
    assert_eq!(
        rolled_back.advance(),
        Err(RecoveryError::TransactionSettled(rolled_back_phase))
    );
}

#[test]
fn canonical_enumeration_is_sorted_deduplicated_and_excludes_internal_namespaces() {
    let objects = vec![
        StoredObject::staging("same"),
        StoredObject::checkpoint("undo"),
        StoredObject::canonical("zeta"),
        StoredObject::trash("same"),
        StoredObject::canonical("alpha"),
        StoredObject::canonical("alpha"),
    ];
    assert_eq!(enumerate_published(objects), vec!["alpha", "zeta"]);
}

proptest! {
    #[test]
    fn generated_failpoints_recover_and_gc_to_identical_fixed_points(
        kind_index in 0u8..4,
        crash_steps in 0u8..20,
        suffix in "[a-z0-9]{1,16}",
    ) {
        let kind = ALL_KINDS[usize::from(kind_index)];
        let observed = observations(kind);
        let mut interrupted = match RecoveryModel::begin(plan(kind, &suffix), observed) {
            BeginOutcome::Started(model) => *model,
            BeginOutcome::Conflict(conflict) => panic!("fresh generated plan conflicted: {conflict:?}"),
        };
        for _ in 0..crash_steps {
            if matches!(interrupted.phase(), TransactionPhase::Complete | TransactionPhase::RolledBack) {
                break;
            }
            interrupted = interrupted.advance().unwrap();
        }

        if let Some(admitted) = interrupted.admitted_authority() {
            prop_assert_eq!(interrupted.accepted_authority(), Some(admitted));
        }
        let recovered_once = interrupted.recover();
        let recovered_twice = recovered_once.clone().recover();
        prop_assert_eq!(&recovered_once, &recovered_twice);
        prop_assert!(!recovered_once.staging_present());

        let restore_undo = recovered_once.retained_checkpoint().cloned();
        let (gc_once, _) = recovered_once.gc_pass();
        let (gc_twice, _) = gc_once.clone().gc_pass();
        prop_assert_eq!(&gc_once, &gc_twice);
        prop_assert_eq!(gc_once.retained_checkpoint(), restore_undo.as_ref());
    }

    #[test]
    fn generated_single_stale_dimensions_never_create_a_prepare_record(
        dimension_index in 0usize..STALE_DIMENSIONS.len(),
        grant_revision in 0u64..u64::MAX,
        checkpoint_revision in 0u64..u64::MAX,
        format_version in 0u32..u32::MAX,
        topology_revision in 0u64..u64::MAX,
    ) {
        let dimension = STALE_DIMENSIONS[dimension_index];
        let mut expected = observations(TransactionKind::Restore);
        expected.grant_revision = grant_revision;
        expected.checkpoint = Some(CheckpointMetadata { revision: checkpoint_revision });
        expected.format = FormatMetadata { version: format_version };
        expected.topology = TopologyMetadata { revision: topology_revision };
        let observed = make_stale(expected.clone(), dimension);
        let checked = CheckedLifecyclePlan::new(
            spec(TransactionKind::Restore, "generated-stale"),
            expected,
        ).unwrap();

        let BeginOutcome::Conflict(conflict) = RecoveryModel::begin(checked, observed) else {
            prop_assert!(false, "{dimension:?} mismatch started execution");
            unreachable!();
        };
        prop_assert_eq!(conflict.stale_dimensions(), &[dimension]);
        prop_assert_eq!(conflict.effect_count(), 0);
        prop_assert_eq!(conflict.phase(), None);
    }

    #[test]
    fn generated_internal_objects_never_enter_enumeration(
        entries in proptest::collection::vec("[a-z0-9-]{1,20}", 0..40),
    ) {
        let mut objects = Vec::with_capacity(entries.len() * 4);
        let mut expected = BTreeSet::new();
        for (index, name) in entries.into_iter().enumerate() {
            objects.push(StoredObject::staging(format!("{name}-{index}")));
            objects.push(StoredObject::checkpoint(format!("{name}-{index}")));
            objects.push(StoredObject::trash(format!("{name}-{index}")));
            if index % 3 == 0 {
                expected.insert(name.clone());
                objects.push(StoredObject::canonical(name));
            }
        }
        prop_assert_eq!(enumerate_published(objects), expected.into_iter().collect::<Vec<_>>());
    }
}
