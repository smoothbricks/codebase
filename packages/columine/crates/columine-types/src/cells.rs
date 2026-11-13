//! Value cells: the seam through which an embedder computes the stored
//! bytes of the cells a plane does not interpret
//! (specs/columine/10-physical-vocabulary.md §7).
//!
//! Four cells of a row are the embedder's when it injects a [`ValueCells`]:
//! a structured member's own Binary column, the record of the row's declared
//! members that have no column, the carrier of its undeclared members, and a
//! text member's content hash with its optional inline bytes. A lane hands
//! each value over as a [`ValueView`] after the semantic schema has judged
//! it, and stores what the embedder writes. This crate holds no codec.

extern crate alloc;

use alloc::vec::Vec;
use core::fmt;

use crate::value::ValueView;

/// Bytes of a text member's content hash.
pub const TEXT_HASH_BYTES: usize = 32;

/// One declared member routed to the row's record: its field-number path
/// into the record, and the position of its field descriptor in the schema
/// the embedder derived the routing from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordRow<'a> {
    pub path: &'a [u16],
    pub descriptor_position: u32,
}

/// One cell an embedder writes, named when a processor is created so a
/// schema the embedder cannot serve is refused before any row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CellTarget<'a> {
    /// A structured member's own Binary column.
    Member { column: u32 },
    /// One member of the declared record.
    Record { row: RecordRow<'a> },
    /// The carrier of undeclared members.
    Carrier { column: u32 },
    /// A text member: its hash column and, when the schema has one, the
    /// Utf8 column for bytes stored inline.
    Text {
        hash_column: u32,
        inline_column: Option<u32>,
    },
}

/// Where a text's bytes live once its hash is stored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextPlace {
    /// The row also stores the bytes, in the text's inline column.
    Inline,
    /// The row stores the hash alone; the embedder keeps the bytes.
    HashOnly,
}

/// Why an embedder did not write a cell. `code` is the embedder's own
/// refusal name, carried to the caller unread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CellRefusal {
    /// The embedder writes no cell of this kind.
    Unsupported,
    /// The value cannot be this cell.
    Refused { code: &'static str },
    /// The memory to write the cell could not be had.
    OutOfMemory,
}

impl fmt::Display for CellRefusal {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => output.write_str("no writer for this cell"),
            Self::Refused { code } => write!(output, "the cell's writer refused the value: {code}"),
            Self::OutOfMemory => output.write_str("the cell could not be backed"),
        }
    }
}

/// An embedder's writer of value cells.
pub trait ValueCells {
    /// The name of the encoding of the Binary cells this writer produces.
    /// Every Binary field it writes says the same name in its `sb.cell`
    /// field metadata, so a reader learns a cell's encoding from the schema.
    fn encoding(&self) -> &'static str;

    /// Called once per target of a schema while a processor is created.
    fn prepare(&mut self, target: CellTarget<'_>) -> Result<(), CellRefusal>;

    /// Append to `out` the cell of the structured member whose Binary
    /// column is `column`.
    fn member(
        &mut self,
        column: u32,
        value: &dyn ValueView,
        out: &mut Vec<u8>,
    ) -> Result<(), CellRefusal>;

    /// Start one row's declared record. A record begun and never finished
    /// is discarded by the next begin.
    fn begin_record(&mut self);

    /// One declared member that has no column.
    fn put(&mut self, row: &RecordRow<'_>, value: &dyn ValueView) -> Result<(), CellRefusal>;

    /// Append the row's record to `out`. `Ok(false)` means no member was
    /// put: nothing is appended and the cell is null.
    fn finish_record(&mut self, out: &mut Vec<u8>) -> Result<bool, CellRefusal>;

    /// Start one row's carrier. A carrier begun and never finished is
    /// discarded by the next begin.
    fn begin_undeclared(&mut self);

    /// One undeclared member, under its path from the payload root.
    fn put_undeclared(&mut self, path: &str, value: &dyn ValueView) -> Result<(), CellRefusal>;

    /// Append the row's carrier to `out`. `Ok(false)` means no member was
    /// put: nothing is appended and the cell is null.
    fn finish_undeclared(&mut self, out: &mut Vec<u8>) -> Result<bool, CellRefusal>;

    /// A text member that arrived as its text: write its content hash to
    /// `hash` and say where its bytes live. Whatever keeping the bytes
    /// outside the row takes is done here.
    fn text(
        &mut self,
        hash_column: u32,
        value: &str,
        hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal>;

    /// A text member that arrived as its content hash alone: answer whether
    /// the embedder holds that content.
    fn text_by_hash(
        &mut self,
        hash_column: u32,
        hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal>;
}

/// The writer of a processor whose embedder injects none: it writes no cell.
/// The lanes then write MessagePack into the three Binary cells themselves,
/// and a schema with a record row or a text row is refused at creation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoValueCells;

impl ValueCells for NoValueCells {
    fn encoding(&self) -> &'static str {
        "msgpack"
    }

    fn prepare(&mut self, target: CellTarget<'_>) -> Result<(), CellRefusal> {
        match target {
            CellTarget::Member { .. } | CellTarget::Carrier { .. } => Ok(()),
            CellTarget::Record { .. } | CellTarget::Text { .. } => Err(CellRefusal::Unsupported),
        }
    }

    fn member(
        &mut self,
        _column: u32,
        _value: &dyn ValueView,
        _out: &mut Vec<u8>,
    ) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn begin_record(&mut self) {}

    fn put(&mut self, _row: &RecordRow<'_>, _value: &dyn ValueView) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn finish_record(&mut self, _out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        Ok(false)
    }

    fn begin_undeclared(&mut self) {}

    fn put_undeclared(&mut self, _path: &str, _value: &dyn ValueView) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn finish_undeclared(&mut self, _out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        Ok(false)
    }

    fn text(
        &mut self,
        _hash_column: u32,
        _value: &str,
        _hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        _hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }
}
