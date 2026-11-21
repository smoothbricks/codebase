//! Re-adoption's resume decisions (02_workspaces.md, "Re-adoption: space reclamation" step 3 and
//! "Missing main image") as pure functions of the facts their callers read: whether an image
//! exists, the record at an id against the re-adoption's own registration, the build records that
//! name `Linked main` and the claim the caller holds. Each answers one step or one refusal, and its
//! caller (`runtime/project_re_adopt.rs`, `storage/apfs/native/re_adopt.rs`) runs exactly that
//! step. Payloads are the caller's: a decision carries what it was given into the step it picks,
//! so no step reaches back into a value a different branch read. The model these are bound to is
//! `specs/cowshed/laws/re_adopt_resume.bend` (`resume/laws.rs`).

use crate::build_volume::{BuildVolumeRecord, Registration};

/// The record found at a build volume id, against the re-adoption's own registration of it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Found {
    Absent,
    Registered,
    Published,
    Foreign,
}

impl Found {
    pub(crate) fn of(found: Option<&BuildVolumeRecord>, registration: &Registration) -> Self {
        match found {
            None => Self::Absent,
            Some(found) if found == registration.registered() => Self::Registered,
            Some(found) if *found == registration.published() => Self::Published,
            Some(_) => Self::Foreign,
        }
    }
}

/// What an empty build volume published under the recorded id does at that id.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EmptyStep {
    /// Nothing is there but, at most, this re-adoption's registration: mint it.
    Create,
    /// An earlier attempt minted it: mount it.
    Mount,
    /// An image no record accounts for.
    Unrecorded,
    /// An image holding another owner's record.
    Foreign,
    /// A record naming a missing image.
    Imageless,
}

/// `image`: an image exists at the id. Nothing is published over an image this re-adoption did
/// not register, and no record is written that names a missing image.
pub(crate) fn empty_step(image: bool, found: Found) -> EmptyStep {
    match (image, found) {
        (false, Found::Absent | Found::Registered) => EmptyStep::Create,
        (false, Found::Published | Found::Foreign) => EmptyStep::Imageless,
        (true, Found::Registered | Found::Published) => EmptyStep::Mount,
        (true, Found::Absent) => EmptyStep::Unrecorded,
        (true, Found::Foreign) => EmptyStep::Foreign,
    }
}

/// What a verified build replacement's publication does at the build image path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplacementStep {
    /// Staged and the path free: register, detach and rename it there.
    Rename,
    /// Renamed by an earlier attempt, which registered it first.
    Published,
    /// Staged, and the path already holds an image.
    Occupied,
    /// The path holds an image under another owner's record.
    Foreign,
    /// The path holds an image no record accounts for.
    Unrecorded,
    /// Neither staged nor placed: nothing to publish, and no record may name nothing.
    Missing,
}

/// `staged`: the staged replacement image exists; `placed`: an image is at the build image path.
pub(crate) fn replacement_step(staged: bool, placed: bool, found: Found) -> ReplacementStep {
    match (staged, placed, found) {
        (true, true, _) => ReplacementStep::Occupied,
        (true, false, _) => ReplacementStep::Rename,
        (false, true, Found::Registered | Found::Published) => ReplacementStep::Published,
        (false, true, Found::Foreign) => ReplacementStep::Foreign,
        (false, true, Found::Absent) => ReplacementStep::Unrecorded,
        (false, false, _) => ReplacementStep::Missing,
    }
}

/// A missing main's build, as its record chose it: none, the recorded one published empty, or a
/// surviving volume kept.
pub(crate) enum MissingBuild<E, K> {
    None,
    Empty(E),
    Kept(K),
}

/// Whose re-adoption this is.
pub(crate) enum Main<E, K> {
    Missing(MissingBuild<E, K>),
    Retained,
}

/// The claim the caller holds on the recorded source build's hold.
pub(crate) enum Claim<C> {
    None,
    /// It names the recorded build volume.
    Naming(C),
    /// It names another.
    Other,
}

/// Which build publication a recorded re-adoption runs.
pub(crate) enum Publication<E, K, B, C> {
    Nothing,
    /// A missing main's recorded build, minted empty under no claim of the caller's.
    EmptyUnclaimed(E),
    /// A missing main's surviving build, kept and seeded under no claim of the caller's.
    KeptUnclaimed(K),
    /// The verified replacement published under the claim, or kept once published.
    Replacement(B, C),
    /// Lost before publication: its staged copy discarded, the build published empty under the claim.
    LostEmptyClaimed(B, C),
    /// The claim does not hold the recorded build volume.
    Invalid,
}

/// `build`: the source build the record names. `retained`: its retained image exists;
/// `published`: an earlier attempt published its verified replacement; neither is read without a
/// build the claim names. Once published, the replacement never reads its retained image again,
/// so losing that image keeps it.
pub(crate) fn publication<E, K, B, C>(
    main: Main<E, K>,
    build: Option<B>,
    claim: Claim<C>,
    retained: bool,
    published: bool,
) -> Publication<E, K, B, C> {
    match (main, build, claim) {
        (Main::Missing(MissingBuild::None) | Main::Retained, None, Claim::None) => {
            Publication::Nothing
        }
        (Main::Missing(MissingBuild::Empty(build)), None, Claim::None) => {
            Publication::EmptyUnclaimed(build)
        }
        (Main::Missing(MissingBuild::Kept(build)), None, Claim::None) => {
            Publication::KeptUnclaimed(build)
        }
        (Main::Retained, Some(build), Claim::Naming(claim)) => {
            if retained || published {
                Publication::Replacement(build, claim)
            } else {
                Publication::LostEmptyClaimed(build, claim)
            }
        }
        _ => Publication::Invalid,
    }
}

/// How many build records name `Linked main`.
pub(crate) enum Linked<O, M> {
    None,
    One(O),
    Many(M),
}

/// A missing main's build.
pub(crate) enum MissingChoice<O, M, R, P> {
    /// Its completed re-adoption names none, and none survives.
    NoBuild,
    /// None survives: the recorded one is published empty.
    Empty(R),
    /// One survives, whatever its id: it is kept.
    Kept(O),
    /// The recorded one is not missing: `P` is still at its id.
    Conflict(R, P),
    /// Several name `Linked main`.
    Integrity(M),
}

/// `recorded`: the build the completed re-adoption names; `occupied`: what is still at that id, an
/// image or a record, read only when no volume survives. A surviving volume is kept whatever its
/// id, and the recorded one is published empty only over nothing.
pub(crate) fn missing_choice<O, M, R, P>(
    linked: Linked<O, M>,
    recorded: Option<R>,
    occupied: Option<P>,
) -> MissingChoice<O, M, R, P> {
    match (linked, recorded, occupied) {
        (Linked::One(kept), _, _) => MissingChoice::Kept(kept),
        (Linked::Many(several), _, _) => MissingChoice::Integrity(several),
        (Linked::None, None, _) => MissingChoice::NoBuild,
        (Linked::None, Some(recorded), None) => MissingChoice::Empty(recorded),
        (Linked::None, Some(recorded), Some(occupant)) => {
            MissingChoice::Conflict(recorded, occupant)
        }
    }
}

/// The build copy before publication.
pub(crate) enum CopyStep<B> {
    Nothing,
    Copy(B),
    /// Its retained image is gone: the staged copy can no longer be verified.
    Discard(B),
}

pub(crate) fn copy_step<B>(build: Option<B>, retained: bool) -> CopyStep<B> {
    match (build, retained) {
        (None, _) => CopyStep::Nothing,
        (Some(build), true) => CopyStep::Copy(build),
        (Some(build), false) => CopyStep::Discard(build),
    }
}

#[cfg(test)]
mod laws;
