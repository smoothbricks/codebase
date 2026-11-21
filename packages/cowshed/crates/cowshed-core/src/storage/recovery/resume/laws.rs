//! The resume decisions bound to their proven model, `specs/cowshed/laws/re_adopt_resume.bend`.
//! Its laws are proven there for every input; each decision's domain here is finite, so this
//! runs the emitted model and the code on all of it and requires the same answer, which carries
//! the laws over to the code. The model's own corpus and seeded draws run as well.

// Regenerate with bun packages/cowshed/scripts/emit-re-adopt-resume-law.ts.
#[rustfmt::skip]
mod model;

use super::{
    Claim, CopyStep, EmptyStep, Found, Linked, Main, MissingBuild, MissingChoice, Publication,
    ReplacementStep,
};

/// One input as the code takes it (made fresh, since payload-carrying inputs are moved) and as the
/// model takes it.
type Case<Code, Model> = (fn() -> Code, Model);
const BOOLS: [bool; 2] = [false, true];
const FOUND: [Found; 4] = [
    Found::Absent,
    Found::Registered,
    Found::Published,
    Found::Foreign,
];

fn rec(found: Found) -> model::Rec {
    match found {
        Found::Absent => model::Rec::RNone,
        Found::Registered => model::Rec::RRegistered,
        Found::Published => model::Rec::RPublished,
        Found::Foreign => model::Rec::RForeign,
    }
}

fn empty(step: EmptyStep) -> model::EmptyStep {
    match step {
        EmptyStep::Create => model::EmptyStep::ECreate,
        EmptyStep::Mount => model::EmptyStep::EMount,
        EmptyStep::Unrecorded => model::EmptyStep::ENoRecord,
        EmptyStep::Foreign => model::EmptyStep::EForeign,
        EmptyStep::Imageless => model::EmptyStep::ENoImage,
    }
}

fn replacement(step: ReplacementStep) -> model::RepStep {
    match step {
        ReplacementStep::Rename => model::RepStep::PRename,
        ReplacementStep::Published => model::RepStep::PAlready,
        ReplacementStep::Occupied => model::RepStep::POccupied,
        ReplacementStep::Foreign => model::RepStep::PForeign,
        ReplacementStep::Unrecorded => model::RepStep::PNoRecord,
        ReplacementStep::Missing => model::RepStep::PNothing,
    }
}

#[test]
fn an_empty_build_takes_the_models_step_at_every_id_state() {
    for image in BOOLS {
        for found in FOUND {
            assert!(
                empty(super::empty_step(image, found)) == model::empty_step(image, rec(found)),
                "image={image} found={found:?}: code {:?}, model {}",
                super::empty_step(image, found),
                model::show_EmptyStep(&model::empty_step(image, rec(found)))
            );
        }
    }
}

#[test]
fn a_replacement_takes_the_models_step_at_every_path_state() {
    for staged in BOOLS {
        for placed in BOOLS {
            for found in FOUND {
                let code = super::replacement_step(staged, placed, found);
                let model = model::rep_step(staged, placed, rec(found));
                assert!(
                    replacement(code) == model,
                    "staged={staged} placed={placed} found={found:?}: code {code:?}, model {}",
                    model::show_RepStep(&model)
                );
            }
        }
    }
}

/// Every combination of a re-adoption's main, its source build and the caller's claim, as code
/// input (payloads are units) and as model input.
fn mains() -> [Case<Main<(), ()>, model::Main>; 4] {
    [
        (
            || Main::Missing(MissingBuild::None),
            model::Main::Missing(model::MissingBuild::MNoBuild),
        ),
        (
            || Main::Missing(MissingBuild::Empty(())),
            model::Main::Missing(model::MissingBuild::MEmpty),
        ),
        (
            || Main::Missing(MissingBuild::Kept(())),
            model::Main::Missing(model::MissingBuild::MKept),
        ),
        (|| Main::Retained, model::Main::Retained),
    ]
}

fn claims() -> [Case<Claim<()>, model::Claim>; 3] {
    [
        (|| Claim::None, model::Claim::CNone),
        (|| Claim::Naming(()), model::Claim::CMatch),
        (|| Claim::Other, model::Claim::COther),
    ]
}

fn publication(answer: Publication<(), (), (), ()>) -> model::Publication {
    match answer {
        Publication::Nothing => model::Publication::BNothing,
        Publication::EmptyUnclaimed(()) => model::Publication::BEmptyUnclaimed,
        Publication::KeptUnclaimed(()) => model::Publication::BKeptUnclaimed,
        Publication::Replacement((), ()) => model::Publication::BReplacement,
        Publication::LostEmptyClaimed((), ()) => model::Publication::BLostEmptyClaimed,
        Publication::Invalid => model::Publication::BInvalid,
    }
}

#[test]
fn a_recorded_re_adoption_takes_the_models_build_publication() {
    for (main, model_main) in mains() {
        for build in BOOLS {
            for (claim, model_claim) in claims() {
                for retained in BOOLS {
                    for published in BOOLS {
                        let code = publication(super::publication(
                            main(),
                            build.then_some(()),
                            claim(),
                            retained,
                            published,
                        ));
                        let model = model::publication(
                            model_main.clone(),
                            build,
                            model_claim.clone(),
                            retained,
                            published,
                        );
                        assert!(
                            code == model,
                            "main={} build={build} claim={} retained={retained} \
                             published={published}: code {}, model {}",
                            model::show_Main(&model_main),
                            model::show_Claim(&model_claim),
                            model::show_Publication(&code),
                            model::show_Publication(&model)
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn a_missing_main_takes_the_models_build() {
    let linked: [Case<Linked<(), ()>, model::Linked>; 3] = [
        (|| Linked::None, model::Linked::LNone),
        (|| Linked::One(()), model::Linked::LOne),
        (|| Linked::Many(()), model::Linked::LMany),
    ];
    for (code_linked, model_linked) in linked {
        for recorded in BOOLS {
            for occupied in BOOLS {
                let code = match super::missing_choice(
                    code_linked(),
                    recorded.then_some(()),
                    occupied.then_some(()),
                ) {
                    MissingChoice::NoBuild => model::MissingChoice::CNoBuild,
                    MissingChoice::Empty(()) => model::MissingChoice::CEmpty,
                    MissingChoice::Kept(()) => model::MissingChoice::CKept,
                    MissingChoice::Conflict((), ()) => model::MissingChoice::CConflict,
                    MissingChoice::Integrity(()) => model::MissingChoice::CIntegrity,
                };
                let model = model::missing_choice(model_linked.clone(), recorded, occupied);
                assert!(
                    code == model,
                    "linked={} recorded={recorded} occupied={occupied}: code {}, model {}",
                    model::show_Linked(&model_linked),
                    model::show_MissingChoice(&code),
                    model::show_MissingChoice(&model)
                );
            }
        }
    }
}

#[test]
fn a_build_copy_takes_the_models_step() {
    for build in BOOLS {
        for retained in BOOLS {
            let code = match super::copy_step(build.then_some(()), retained) {
                CopyStep::Nothing => model::Copy::KNothing,
                CopyStep::Copy(()) => model::Copy::KCopy,
                CopyStep::Discard(()) => model::Copy::KDiscard,
            };
            assert!(
                code == model::copy_step(build, retained),
                "build={build} retained={retained}: code {}, model {}",
                model::show_Copy(&code),
                model::show_Copy(&model::copy_step(build, retained))
            );
        }
    }
}

/// The model's laws on its own corpus: the proofs' statements, evaluated.
#[test]
fn the_models_laws_hold_on_its_corpus() {
    let mut failures = Vec::new();
    let mut note = |law: &str, (ok, shown): (bool, String)| {
        if !ok {
            failures.push(format!("{law}: {shown}"));
        }
    };
    for (rec,) in model::corpus_empty_never_over_a_foreign_image() {
        note(
            "empty_never_over_a_foreign_image",
            model::law_empty_never_over_a_foreign_image(rec),
        );
    }
    for (staged, rec) in model::corpus_replacement_never_over_a_foreign_image() {
        note(
            "replacement_never_over_a_foreign_image",
            model::law_replacement_never_over_a_foreign_image(staged, rec),
        );
    }
    for (rec,) in model::corpus_empty_no_record_names_nothing() {
        note(
            "empty_no_record_names_nothing",
            model::law_empty_no_record_names_nothing(rec),
        );
    }
    for (rec,) in model::corpus_replacement_no_record_names_nothing() {
        note(
            "replacement_no_record_names_nothing",
            model::law_replacement_no_record_names_nothing(rec),
        );
    }
    for (rec,) in model::corpus_empty_replay_converges() {
        note(
            "empty_replay_converges",
            model::law_empty_replay_converges(rec),
        );
    }
    for (rec,) in model::corpus_replacement_replay_converges() {
        note(
            "replacement_replay_converges",
            model::law_replacement_replay_converges(rec),
        );
    }
    for (retained,) in model::corpus_published_replacement_is_kept() {
        note(
            "published_replacement_is_kept",
            model::law_published_replacement_is_kept(retained),
        );
    }
    for (claim, retained, published) in model::corpus_a_retained_build_needs_its_claim() {
        note(
            "a_retained_build_needs_its_claim",
            model::law_a_retained_build_needs_its_claim(claim, retained, published),
        );
    }
    for (build, retained, published) in model::corpus_a_missing_main_holds_no_claim() {
        note(
            "a_missing_main_holds_no_claim",
            model::law_a_missing_main_holds_no_claim(build, retained, published),
        );
    }
    for (occupied,) in model::corpus_a_missing_main_never_publishes_over_a_volume() {
        note(
            "a_missing_main_never_publishes_over_a_volume",
            model::law_a_missing_main_never_publishes_over_a_volume(occupied),
        );
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
