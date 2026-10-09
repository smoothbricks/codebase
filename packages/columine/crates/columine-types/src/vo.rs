//! Value-object admission capability: the one shared model.
//!
//! These are data and capability declarations only. No value-object kernel,
//! frame codec, or schema decoder lives here; a runtime injects a
//! [`VoAdmission`] capability that owns the real kernel. Payload and program
//! execution consumers judge frames through the same types and
//! [`admit_frame`] proof check; loaders prepare schemas and certify roles.
//!
//! The protocol: [`VoAdmission::prepare`] runs once per declared
//! [`VoMetadata`] before any consumer state exists ([`prepare_each`]).
//! [`VoAdmission::admit`] judges one borrowed frame and returns the
//! capability's own proof. [`admit_frame`] never trusts a positive answer
//! alone: the proof must name the very slice it was lent and the declared
//! root and frame. A frame written under a retained writer schema is
//! certified as the kernel's projection into the reader's layout, so the
//! certified bytes may be owned by the proof rather than borrowed from the
//! input. [`UnavailableVoAdmission`] is the explicit refusal capability — not
//! a no-op — so a consumer without a kernel refuses value objects outright.
//!
//! Programs extend the protocol through [`VoProgramAdmission`]. A program's
//! [`VoProgramMetadata`] links each value-object struct-map field to its
//! schema and gives each counts instruction the schema field ids of its
//! count, histograms, and masks. The kernel derives the physical
//! [`VoCountsLayout`] those ids occupy in the schema's frames;
//! [`certify_counts`] checks the proof names the role and schema it was
//! asked about, and the loader compares the certified layout with the
//! instruction's operands before any program state exists.

extern crate alloc;

use alloc::{string::String, vec::Vec};
use core::{convert::Infallible, fmt};

/// Canonical value-object schema metadata.
///
/// `root` is the qualified record name (`namespace.Record`); `frame` is the
/// canonical frame table; `rows` are the canonical manifest rows (ordinal,
/// name, commitment, descriptor) in dependency and ordinal order. A bare
/// descriptor is not metadata: it cannot resolve nested shapes. `profile`
/// carries the committed definitions the rows' commitments are checked
/// against; it is evidence for the capability, never trusted on its own.
/// `retained` is the family's canonical retained manifest rows, the writer
/// schemas a frame written under an earlier table resolves against. Empty
/// means only that no retained history was supplied. Resolution belongs to
/// the capability; this crate carries the rows without decoding them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoMetadata {
    pub root: String,
    pub frame: Vec<u8>,
    pub rows: Vec<Vec<u8>>,
    pub profile: VoProfile,
    pub retained: Vec<Vec<u8>>,
}

/// The committed definitions behind a root's rows: every record in the
/// root's closure and every attribute those records' properties name. The
/// capability verifies each definition against the commitment its row
/// carries and derives any refinement from the verified definition alone; a
/// definition that does not match its commitment proves nothing. This crate
/// neither parses nor hashes those bytes, and provides no JSON-hash fallback.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VoProfile {
    pub records: Vec<VoDefinition>,
    pub attributes: Vec<VoDefinition>,
}

/// One named definition, exactly as committed: its canonicalVO1 binary encoding
/// against its kind's meta-schema, the bytes its commitment is computed over.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoDefinition {
    pub name: String,
    pub definition: Vec<u8>,
}

/// The schema a value-object proof certifies: its root, its frame table, and
/// the ordinal the capability bound that root to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VoSchemaRef<'a> {
    pub root: &'a str,
    pub frame: &'a [u8],
    pub ordinal: u32,
}

/// A capability's proof that one frame was admitted.
pub trait VoFrameProof {
    /// The slice lent to [`VoAdmission::admit`], exactly — same address and
    /// length. [`admit_frame`] always requires it.
    fn lent(&self) -> &[u8];

    /// The certified frame in the reader's layout: [`VoFrameProof::lent`]
    /// itself when the frame's table is the reader's, otherwise the kernel's
    /// projection, owned by the proof.
    fn bytes(&self) -> &[u8];

    /// The reader schema the certified bytes are in.
    fn schema_ref(&self) -> VoSchemaRef<'_>;
}

/// The value-object admission capability.
pub trait VoAdmission {
    type Frame<'a>: VoFrameProof
    where
        Self: 'a;

    /// Prepare one declared schema, once, before any consumer state exists.
    fn prepare(&mut self, metadata: &VoMetadata) -> Result<(), VoAdmissionError>;

    /// Judge one borrowed frame against prepared metadata.
    fn admit<'a>(
        &'a self,
        metadata: &'a VoMetadata,
        bytes: &'a [u8],
    ) -> Result<Self::Frame<'a>, VoAdmissionError>;
}

/// Why a capability refused to prepare metadata or admit a frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoAdmissionError {
    /// No value-object capability is present.
    Unavailable,
    /// The root was never prepared, or does not resolve in its rows.
    UnknownRoot,
    /// The frame belongs to a schema other than the declared one.
    SchemaMismatch,
    InvalidSchema {
        rule: &'static str,
        message: &'static str,
        at: u32,
    },
    InvalidFrame {
        rule: &'static str,
        message: &'static str,
        at: u32,
    },
}

impl fmt::Display for VoAdmissionError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => output.write_str("value-object admission unavailable"),
            Self::UnknownRoot => output.write_str("unknown value-object root"),
            Self::SchemaMismatch => output.write_str("value-object schema mismatch"),
            Self::InvalidSchema { rule, message, at } => {
                write!(
                    output,
                    "invalid value-object schema [{rule}] at {at}: {message}"
                )
            }
            Self::InvalidFrame { rule, message, at } => {
                write!(
                    output,
                    "invalid value-object frame [{rule}] at {at}: {message}"
                )
            }
        }
    }
}

/// Why a consumer refused a value-object frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoRefusal {
    /// The capability refused the frame.
    Admission(VoAdmissionError),
    /// The proof certified bytes other than the borrowed frame.
    SubstitutedBytes,
    /// The proof names a root other than the declared one.
    RootMismatch,
    /// The proof names a frame table other than the declared one.
    FrameMismatch,
}

impl fmt::Display for VoRefusal {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => fmt::Display::fmt(error, output),
            Self::SubstitutedBytes => {
                output.write_str("admission proof certified bytes other than the frame")
            }
            Self::RootMismatch => output.write_str("admission proof names another root"),
            Self::FrameMismatch => output.write_str("admission proof names another frame"),
        }
    }
}

/// Why a capability refused to prepare one declared value-object schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoPreparationError {
    pub root: String,
    pub error: VoAdmissionError,
}

/// Prepare `admission` once for each distinct metadata in `declared`, in
/// order; a repeat of an already prepared metadata is skipped. The first
/// refusal stops preparation and names its root. Returns how many distinct
/// schemas were prepared, so a consumer can tell an empty declaration from a
/// prepared one.
pub fn prepare_each<'m, A: VoAdmission>(
    admission: &mut A,
    declared: impl IntoIterator<Item = &'m VoMetadata>,
) -> Result<usize, VoPreparationError> {
    let mut prepared: Vec<&VoMetadata> = Vec::new();
    for metadata in declared {
        if prepared.contains(&metadata) {
            continue;
        }
        admission
            .prepare(metadata)
            .map_err(|error| VoPreparationError {
                root: metadata.root.clone(),
                error,
            })?;
        prepared.push(metadata);
    }
    Ok(prepared.len())
}

/// Admit `bytes` through `admission` and check its proof: it must name the
/// lent slice itself (address and length, not equal contents), and the
/// declared root and frame. The proof is returned so consumers write its
/// certified [`VoFrameProof::bytes`]; it is never trusted unchecked.
pub fn admit_frame<'a, A: VoAdmission>(
    admission: &'a A,
    metadata: &'a VoMetadata,
    bytes: &'a [u8],
) -> Result<A::Frame<'a>, VoRefusal> {
    let proof = admission
        .admit(metadata, bytes)
        .map_err(VoRefusal::Admission)?;
    check_proof(&proof, metadata, bytes)?;
    Ok(proof)
}

/// Check that `proof` certifies `bytes` itself (the lent slice, same address
/// and length) under the declared root and frame table of `metadata`.
fn check_proof<P: VoFrameProof>(
    proof: &P,
    metadata: &VoMetadata,
    bytes: &[u8],
) -> Result<(), VoRefusal> {
    let schema = proof.schema_ref();
    if !core::ptr::eq(proof.lent(), bytes) {
        Err(VoRefusal::SubstitutedBytes)
    } else if schema.root != metadata.root {
        Err(VoRefusal::RootMismatch)
    } else if schema.frame != metadata.frame.as_slice() {
        Err(VoRefusal::FrameMismatch)
    } else {
        Ok(())
    }
}

/// One value-object struct-map field of a program and the schema its frames
/// carry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoFieldLink {
    pub slot: u32,
    pub field_idx: u8,
    pub schema: VoMetadata,
}

/// The schema roles of one counts instruction, by schema field id.
///
/// The instruction itself names the struct-map field it folds into, and
/// through its [`VoFieldLink`] the schema these ids belong to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoCountsRole {
    /// Byte offset of the instruction's opcode in the program bytecode.
    pub instruction_offset: u32,
    /// The `u32` sample-count field.
    pub count_field: u16,
    /// The `u16` histogram fields, in instruction order.
    pub histogram_fields: Vec<u16>,
    /// The `u8` OR-mask fields, in instruction order.
    pub mask_fields: Vec<u16>,
}

/// A program's value-object declarations, carried beside its bytecode.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VoProgramMetadata {
    pub fields: Vec<VoFieldLink>,
    pub counts: Vec<VoCountsRole>,
}

impl VoProgramMetadata {
    /// Every schema the program declares, repeats included; feed it to
    /// [`prepare_each`].
    pub fn schemas(&self) -> impl Iterator<Item = &VoMetadata> {
        self.fields.iter().map(|link| &link.schema)
    }

    /// The schema linked to one struct-map field.
    pub fn schema_for(&self, slot: u32, field_idx: u8) -> Option<&VoMetadata> {
        self.fields
            .iter()
            .find(|link| link.slot == slot && link.field_idx == field_idx)
            .map(|link| &link.schema)
    }
}

/// Where one histogram lives in every frame of its schema, in the counts
/// instruction's coordinates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VoHistogramLayout {
    /// Offset of the histogram's `u32` region word, from the frame origin.
    pub ref_off: u32,
    /// Offset of bin zero from the region start.
    pub elems_at: u8,
    pub bin_count: u8,
}

/// The physical positions a [`VoCountsRole`] occupies in every frame of its
/// schema, in the counts instruction's coordinates: the frame origin is
/// `prefix.len()`. Offsets are relative to it except the histogram's `elems_at`,
/// which is relative to that histogram's region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoCountsLayout {
    /// The leading bytes (root header and padding) every frame of the schema
    /// begins with.
    pub prefix: Vec<u8>,
    pub count_off: u32,
    /// One per [`VoCountsRole::histogram_fields`] entry, in that order.
    pub histograms: Vec<VoHistogramLayout>,
    /// One per [`VoCountsRole::mask_fields`] entry, in that order.
    pub mask_offs: Vec<u32>,
}

/// A kernel's proof of where one counts role lives in its schema's frames.
pub trait VoCountsProof {
    /// The role certified. [`certify_counts`] requires the very role it lent
    /// to [`VoProgramAdmission::certify_counts`].
    fn role(&self) -> &VoCountsRole;

    fn schema_ref(&self) -> VoSchemaRef<'_>;

    fn layout(&self) -> &VoCountsLayout;
}

/// The program side of the capability: certify, against a prepared schema,
/// that a counts role's field ids are a `u32` count, `u16` histograms with
/// fixed bin counts, and `u8` masks, and where they lie.
pub trait VoProgramAdmission: VoAdmission {
    type Counts<'a>: VoCountsProof
    where
        Self: 'a;

    fn certify_counts<'a>(
        &'a self,
        metadata: &'a VoMetadata,
        role: &'a VoCountsRole,
    ) -> Result<Self::Counts<'a>, VoAdmissionError>;
}

/// Why a program loader refused a value-object declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoProgramRefusal {
    /// The capability refused the role.
    Admission(VoAdmissionError),
    /// The proof certified a role other than the one lent.
    SubstitutedRole,
    /// The proof names a root other than the declared one.
    RootMismatch,
    /// The proof names a frame table other than the declared one.
    FrameMismatch,
}

impl fmt::Display for VoProgramRefusal {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => fmt::Display::fmt(error, output),
            Self::SubstitutedRole => {
                output.write_str("counts proof certified a role other than the declared one")
            }
            Self::RootMismatch => output.write_str("counts proof names another root"),
            Self::FrameMismatch => output.write_str("counts proof names another frame"),
        }
    }
}

/// Certify `role` against `metadata` and check the proof names that very
/// role (address, not equal contents) under the declared root and frame. The
/// loader still owes the comparison of [`VoCountsProof::layout`] with the
/// decoded instruction at [`VoCountsRole::instruction_offset`].
pub fn certify_counts<'a, A: VoProgramAdmission>(
    admission: &'a A,
    metadata: &'a VoMetadata,
    role: &'a VoCountsRole,
) -> Result<A::Counts<'a>, VoProgramRefusal> {
    let proof = admission
        .certify_counts(metadata, role)
        .map_err(VoProgramRefusal::Admission)?;
    let schema = proof.schema_ref();
    if !core::ptr::eq(proof.role(), role) {
        Err(VoProgramRefusal::SubstitutedRole)
    } else if schema.root != metadata.root {
        Err(VoProgramRefusal::RootMismatch)
    } else if schema.frame != metadata.frame.as_slice() {
        Err(VoProgramRefusal::FrameMismatch)
    } else {
        Ok(proof)
    }
}

/// The explicit refusal capability: preparation, admission, and counts
/// certification always answer [`VoAdmissionError::Unavailable`], and its
/// proof types are uninhabited.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UnavailableVoAdmission;

impl VoFrameProof for Infallible {
    fn lent(&self) -> &[u8] {
        match *self {}
    }

    fn bytes(&self) -> &[u8] {
        match *self {}
    }

    fn schema_ref(&self) -> VoSchemaRef<'_> {
        match *self {}
    }
}

impl VoAdmission for UnavailableVoAdmission {
    type Frame<'a>
        = Infallible
    where
        Self: 'a;

    fn prepare(&mut self, _metadata: &VoMetadata) -> Result<(), VoAdmissionError> {
        Err(VoAdmissionError::Unavailable)
    }

    fn admit<'a>(
        &'a self,
        _metadata: &'a VoMetadata,
        _bytes: &'a [u8],
    ) -> Result<Infallible, VoAdmissionError> {
        Err(VoAdmissionError::Unavailable)
    }
}

impl VoCountsProof for Infallible {
    fn role(&self) -> &VoCountsRole {
        match *self {}
    }

    fn schema_ref(&self) -> VoSchemaRef<'_> {
        match *self {}
    }

    fn layout(&self) -> &VoCountsLayout {
        match *self {}
    }
}

impl VoProgramAdmission for UnavailableVoAdmission {
    type Counts<'a>
        = Infallible
    where
        Self: 'a;

    fn certify_counts<'a>(
        &'a self,
        _metadata: &'a VoMetadata,
        _role: &'a VoCountsRole,
    ) -> Result<Infallible, VoAdmissionError> {
        Err(VoAdmissionError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    use super::*;

    fn metadata(root: &str) -> VoMetadata {
        VoMetadata {
            root: root.into(),
            frame: alloc::vec![0x0a],
            rows: alloc::vec![alloc::vec![0x00]],
            profile: VoProfile::default(),
            retained: alloc::vec![],
        }
    }

    #[test]
    fn the_unavailable_capability_refuses_every_request() {
        let line = metadata("orders.Line");
        assert_eq!(
            prepare_each(&mut UnavailableVoAdmission, [&line, &line]),
            Err(VoPreparationError {
                root: "orders.Line".into(),
                error: VoAdmissionError::Unavailable,
            })
        );
        assert_eq!(
            prepare_each(&mut UnavailableVoAdmission, core::iter::empty()),
            Ok(0)
        );
        assert_eq!(
            admit_frame(&UnavailableVoAdmission, &line, &[1, 2]).err(),
            Some(VoRefusal::Admission(VoAdmissionError::Unavailable))
        );
        let role = VoCountsRole {
            instruction_offset: 0,
            count_field: 1,
            histogram_fields: alloc::vec![2],
            mask_fields: alloc::vec![],
        };
        assert_eq!(
            certify_counts(&UnavailableVoAdmission, &line, &role).err(),
            Some(VoProgramRefusal::Admission(VoAdmissionError::Unavailable))
        );
    }

    #[test]
    fn program_metadata_links_fields_to_schemas() {
        let program = VoProgramMetadata {
            fields: alloc::vec![
                VoFieldLink {
                    slot: 0,
                    field_idx: 2,
                    schema: metadata("orders.Line"),
                },
                VoFieldLink {
                    slot: 1,
                    field_idx: 0,
                    schema: metadata("orders.Line"),
                },
            ],
            counts: alloc::vec![],
        };
        assert_eq!(program.schemas().count(), 2);
        assert_eq!(
            program.schema_for(1, 0).map(|schema| schema.root.as_str()),
            Some("orders.Line")
        );
        assert_eq!(program.schema_for(0, 0), None);
    }

    /// A proof value for [`check_proof`]: data the checker must refuse, never a
    /// capability. No test here admits a frame.
    struct Named<'a> {
        lent: &'a [u8],
        root: &'a str,
        frame: &'a [u8],
    }

    impl VoFrameProof for Named<'_> {
        fn lent(&self) -> &[u8] {
            self.lent
        }

        fn bytes(&self) -> &[u8] {
            self.lent
        }

        fn schema_ref(&self) -> VoSchemaRef<'_> {
            VoSchemaRef {
                root: self.root,
                frame: self.frame,
                ordinal: 0,
            }
        }
    }

    #[test]
    fn a_proof_naming_other_bytes_root_or_frame_is_refused() {
        let line = metadata("orders.Line");
        let input = [1_u8, 2, 3];
        let equal = [1_u8, 2, 3];
        assert_eq!(
            check_proof(
                &Named {
                    lent: &equal,
                    root: "orders.Line",
                    frame: &[0x0a],
                },
                &line,
                &input,
            ),
            Err(VoRefusal::SubstitutedBytes),
            "equal contents at another address are not the lent frame"
        );
        assert_eq!(
            check_proof(
                &Named {
                    lent: &input[..2],
                    root: "orders.Line",
                    frame: &[0x0a],
                },
                &line,
                &input,
            ),
            Err(VoRefusal::SubstitutedBytes),
            "a prefix of the lent frame is not the lent frame"
        );
        assert_eq!(
            check_proof(
                &Named {
                    lent: &input,
                    root: "orders.Other",
                    frame: &[0x0a],
                },
                &line,
                &input,
            ),
            Err(VoRefusal::RootMismatch)
        );
        assert_eq!(
            check_proof(
                &Named {
                    lent: &input,
                    root: "orders.Line",
                    frame: &[0x0b],
                },
                &line,
                &input,
            ),
            Err(VoRefusal::FrameMismatch)
        );
    }
}
