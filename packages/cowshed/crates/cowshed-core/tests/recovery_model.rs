use std::collections::BTreeSet;

use cowshed_core::metadata::WorkspaceIncarnation;
use cowshed_core::storage::recovery::{
    Authority, Failpoint, GcPass, Generation, MetadataGeneration, ObjectNamespace, ProtocolEvent,
    RecoveryDisposition, RecoveryError, RecoveryModel, StoredObject, TransactionKind,
    TransactionPhase, TransactionSpec, enumerate_published,
};
use proptest::prelude::*;

fn authority(digit: char, token: &str) -> Authority {
    Authority::new(
        WorkspaceIncarnation::new(digit.to_string().repeat(32)).unwrap(),
        token,
    )
    .unwrap()
}

fn spec(kind: TransactionKind, suffix: &str) -> TransactionSpec {
    let old = authority('a', &format!("old-{suffix}"));
    let new = authority('b', &format!("new-{suffix}"));
    let (old, new) = match kind {
        TransactionKind::Adopt | TransactionKind::Create => (None, Some(new)),
        TransactionKind::Restore => (Some(old), Some(new)),
        TransactionKind::Retire => (Some(old), None),
    };
    TransactionSpec::new(kind, "topic", format!("tx-{suffix}"), old, new).unwrap()
}

fn initial(kind: TransactionKind) -> RecoveryModel {
    RecoveryModel::begin(spec(kind, "fixture"))
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
    match kind {
        TransactionKind::Adopt | TransactionKind::Create => None,
        TransactionKind::Restore | TransactionKind::Retire => Some(Generation::Old),
    }
}

fn expected_new_authority(kind: TransactionKind) -> Option<Generation> {
    match kind {
        TransactionKind::Adopt | TransactionKind::Create | TransactionKind::Restore => {
            Some(Generation::New)
        }
        TransactionKind::Retire => None,
    }
}

fn event_position(events: &[ProtocolEvent], expected: &ProtocolEvent) -> usize {
    events.iter().position(|event| event == expected).unwrap()
}

#[test]
fn transaction_specs_require_the_authority_shape_and_fresh_restore_credentials() {
    let old_authority = authority('a', "old");
    let new_authority = authority('b', "new");

    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Adopt,
            "main",
            "tx",
            Some(old_authority.clone()),
            Some(new_authority.clone()),
        ),
        Err(RecoveryError::UnexpectedOldAuthority(
            TransactionKind::Adopt
        ))
    );
    assert_eq!(
        TransactionSpec::new(TransactionKind::Create, "ws", "tx", None, None),
        Err(RecoveryError::MissingNewAuthority(TransactionKind::Create))
    );
    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Restore,
            "ws",
            "tx",
            None,
            Some(new_authority.clone()),
        ),
        Err(RecoveryError::MissingOldAuthority(TransactionKind::Restore))
    );
    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Retire,
            "ws",
            "tx",
            None,
            Some(new_authority.clone()),
        ),
        Err(RecoveryError::MissingOldAuthority(TransactionKind::Retire))
    );
    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Retire,
            "ws",
            "tx",
            Some(old_authority.clone()),
            Some(new_authority.clone()),
        ),
        Err(RecoveryError::UnexpectedNewAuthority(
            TransactionKind::Retire
        ))
    );
    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Restore,
            "ws",
            "tx",
            Some(old_authority.clone()),
            Some(authority('a', "different")),
        ),
        Err(RecoveryError::ReusedIncarnation)
    );
    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Restore,
            "ws",
            "tx",
            Some(old_authority.clone()),
            Some(authority('b', "old")),
        ),
        Err(RecoveryError::ReusedToken)
    );
    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Restore,
            "",
            "tx",
            Some(old_authority.clone()),
            Some(new_authority.clone()),
        ),
        Err(RecoveryError::EmptyLogicalName)
    );
    assert_eq!(
        TransactionSpec::new(
            TransactionKind::Restore,
            "ws",
            "",
            Some(old_authority.clone()),
            Some(new_authority),
        ),
        Err(RecoveryError::EmptyTransactionId)
    );
    assert_eq!(
        Authority::new(WorkspaceIncarnation::new("c".repeat(32)).unwrap(), ""),
        Err(RecoveryError::EmptyToken)
    );
}

#[test]
fn spec_and_object_accessors_expose_every_recoverable_record_field() {
    let record = spec(TransactionKind::Restore, "fields");
    assert_eq!(record.kind(), TransactionKind::Restore);
    assert_eq!(record.logical_name(), "topic");
    assert_eq!(record.transaction_id(), "tx-fields");
    assert_eq!(record.old_authority().unwrap().token(), "old-fields");
    assert_eq!(
        record.old_authority().unwrap().incarnation().as_str(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(record.new_authority().unwrap().token(), "new-fields");
    assert_eq!(
        record.new_authority().unwrap().incarnation().as_str(),
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    );

    let staging = record.staging_object().unwrap();
    assert_eq!(staging.namespace(), ObjectNamespace::Staging);
    assert_eq!(staging.name(), ".staging/topic-tx-fields");
    let cleanup = record.cleanup_object().unwrap();
    assert_eq!(cleanup.namespace(), ObjectNamespace::Trash);
    assert_eq!(cleanup.name(), ".trash/topic-tx-fields");
    assert_eq!(
        spec(TransactionKind::Adopt, "no-old").cleanup_object(),
        None
    );
    assert_eq!(
        spec(TransactionKind::Retire, "no-new").staging_object(),
        None
    );
}

#[test]
fn restore_orders_incarnation_token_flush_publication_fsync_cutover_and_admission() {
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
fn reusable_failpoint_fixture_materializes_exact_recoverable_records() {
    let failpoint = Failpoint::after(TransactionPhase::TokenMinted);
    assert_eq!(failpoint.phase(), TransactionPhase::TokenMinted);
    let interrupted = failpoint
        .interrupt(spec(TransactionKind::Restore, "failpoint"))
        .unwrap();
    assert_eq!(interrupted.phase(), TransactionPhase::TokenMinted);
    assert_eq!(interrupted.spec().kind(), TransactionKind::Restore);
    assert_eq!(
        Failpoint::after(TransactionPhase::TokenMinted)
            .interrupt(spec(TransactionKind::Retire, "unreachable")),
        Err(RecoveryError::UnreachableFailpoint {
            kind: TransactionKind::Retire,
            phase: TransactionPhase::TokenMinted,
        })
    );
}

#[test]
fn prepare_materializes_only_replacement_staging() {
    let create = initial(TransactionKind::Create).advance().unwrap();
    assert_eq!(create.phase(), TransactionPhase::Prepared);
    assert!(create.staging_present());

    let retire = initial(TransactionKind::Retire).advance().unwrap();
    assert_eq!(retire.phase(), TransactionPhase::Prepared);
    assert!(!retire.staging_present());

    let retire = advance_to(retire, TransactionPhase::Validated)
        .advance()
        .unwrap();
    assert_eq!(retire.phase(), TransactionPhase::CanonicalSwapped);
    assert!(
        !retire
            .events()
            .iter()
            .any(|event| event == &ProtocolEvent::IncarnationMinted)
    );
}

#[test]
fn retire_revokes_old_authority_at_publication_and_never_admits_a_replacement() {
    let published = advance_to(
        initial(TransactionKind::Retire),
        TransactionPhase::Published,
    );
    assert_eq!(published.canonical(), MetadataGeneration::Absent);
    assert_eq!(published.metadata(), MetadataGeneration::Absent);
    assert_eq!(published.synced_metadata(), MetadataGeneration::Absent);
    assert_eq!(published.accepted_authority(), None);
    assert_eq!(published.admitted_authority(), None);
    assert_eq!(
        published.events().last(),
        Some(&ProtocolEvent::AuthorityCutover {
            from: Some(Generation::Old),
            to: None,
        })
    );
    assert!(
        !published
            .events()
            .iter()
            .any(|event| event == &ProtocolEvent::NewAdmission)
    );
    let cleanup = published.advance().unwrap();
    assert_eq!(cleanup.phase(), TransactionPhase::CleanupPending);
    assert_eq!(cleanup.admitted_authority(), None);
    assert!(
        !cleanup
            .events()
            .iter()
            .any(|event| event == &ProtocolEvent::NewAdmission)
    );
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
fn all_interrupted_adopt_create_restore_and_retire_records_converge_idempotently() {
    for kind in [
        TransactionKind::Adopt,
        TransactionKind::Create,
        TransactionKind::Restore,
        TransactionKind::Retire,
    ] {
        let mut interrupted = initial(kind);
        loop {
            let crash_phase = interrupted.phase();
            let direction = crash_phase.recovery_disposition();
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
                    let expected_phase =
                        if matches!(kind, TransactionKind::Restore | TransactionKind::Retire) {
                            TransactionPhase::CleanupPending
                        } else {
                            TransactionPhase::Complete
                        };
                    assert_eq!(recovered_once.phase(), expected_phase);
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
}

#[test]
fn gc_only_reclaims_recovery_classified_debris_and_is_idempotent() {
    let swapped = advance_to(
        initial(TransactionKind::Restore),
        TransactionPhase::CanonicalSwapped,
    );
    assert!(swapped.cleanup_present());
    let (untouched, report) = swapped.clone().gc_pass();
    assert_eq!(untouched, swapped);
    assert_eq!(report, GcPass::default());

    let recovery_classified = advance_to(swapped, TransactionPhase::Published).recover();
    assert_eq!(
        recovery_classified.phase(),
        TransactionPhase::CleanupPending
    );
    let (collected_once, first) = recovery_classified.gc_pass();
    assert_eq!(
        first,
        GcPass {
            examined: 1,
            reclaimed: 1
        }
    );
    assert_eq!(collected_once.phase(), TransactionPhase::Complete);
    assert!(!collected_once.cleanup_present());
    assert_eq!(
        collected_once.events().last(),
        Some(&ProtocolEvent::CleanupReclaimed)
    );
    let (collected_twice, second) = collected_once.clone().gc_pass();
    assert_eq!(collected_twice, collected_once);
    assert_eq!(second, GcPass::default());
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
fn canonical_enumeration_is_sorted_deduplicated_and_excludes_hidden_namespaces() {
    let objects = vec![
        StoredObject::staging("same"),
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
        crash_steps in 0u8..16,
        suffix in "[a-z0-9]{1,16}",
    ) {
        let kind = [
            TransactionKind::Adopt,
            TransactionKind::Create,
            TransactionKind::Restore,
            TransactionKind::Retire,
        ][usize::from(kind_index)];
        let mut interrupted = RecoveryModel::begin(spec(kind, &suffix));
        for _ in 0..crash_steps {
            if matches!(interrupted.phase(), TransactionPhase::Complete | TransactionPhase::RolledBack) {
                break;
            }
            interrupted = interrupted.advance().unwrap();
        }

        let recovered_once = interrupted.recover();
        let recovered_twice = recovered_once.clone().recover();
        prop_assert_eq!(&recovered_once, &recovered_twice);
        prop_assert!(!recovered_once.staging_present());

        let (gc_once, _) = recovered_once.gc_pass();
        let (gc_twice, _) = gc_once.clone().gc_pass();
        prop_assert_eq!(gc_once, gc_twice);
    }

    #[test]
    fn generated_hidden_objects_never_enter_enumeration(
        entries in proptest::collection::vec("[a-z0-9-]{1,20}", 0..40),
    ) {
        let mut objects = Vec::with_capacity(entries.len() * 3);
        let mut expected = BTreeSet::new();
        for (index, name) in entries.into_iter().enumerate() {
            objects.push(StoredObject::staging(format!("{name}-{index}")));
            objects.push(StoredObject::trash(format!("{name}-{index}")));
            if index % 3 == 0 {
                expected.insert(name.clone());
                objects.push(StoredObject::canonical(name));
            }
        }
        prop_assert_eq!(enumerate_published(objects), expected.into_iter().collect::<Vec<_>>());
    }
}
