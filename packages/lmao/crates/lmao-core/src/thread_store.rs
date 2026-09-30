//! A [`ThreadSpanBuffer`] together with the schema it was built from.
//!
//! A buffer borrows its [`FieldMeta`] table for its whole life. A Rust schema
//! is `'static` generated data; a schema a foreign writer describes at run time
//! arrives as a compact blob and has to be owned by someone that outlives the
//! buffer. [`ThreadStore`] is that owner, shared by every binding — Wasm,
//! native C ABI, and any host that embeds the row store — so the blob grammar
//! and its lifetime handling exist once.
//!
//! Blob grammar: each field is `[kind:u8][name_len:u8][name bytes]`; an enum
//! field appends `[variant_count:u16 LE]` and `[len:u8][variant bytes]` per
//! variant. Kinds are the generated `ATTRIBUTE_KIND_*` discriminants. Names and
//! variants are non-empty, unique, UTF-8.

use std::collections::HashSet;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

use crate::columns::{FieldMeta, FieldStrategy};
use crate::thread_buffer::ThreadSpanBuffer;
use crate::thread_kinds::{
    ATTRIBUTE_KIND_BOOLEAN, ATTRIBUTE_KIND_ENUM, ATTRIBUTE_KIND_NUMBER, ATTRIBUTE_KIND_TEXT,
    ATTRIBUTE_KIND_UINT64,
};
use crate::thread_schema::SYSTEM_COLUMN_COUNT;

enum ParsedStrategy {
    Number,
    Uint64,
    Boolean,
    Text,
    Enum(Vec<Box<str>>),
}

/// Owns the strings a run-time schema's `&'static` metadata points into.
struct SchemaStorage {
    fields: Box<[FieldMeta]>,
    _names: Vec<Box<str>>,
    _variants: Vec<Vec<Box<str>>>,
    _variant_tables: Vec<Box<[&'static str]>>,
}

/// Extend a borrow of heap data this storage owns to `'static`.
///
/// # Safety
/// The referent must be owned by a [`SchemaStorage`] that outlives every use of
/// the returned reference. [`ThreadStore`] guarantees it by dropping its buffer
/// — the only holder of these references — before the storage.
unsafe fn extend<T: ?Sized>(value: &T) -> &'static T {
    // SAFETY: the caller's contract above.
    unsafe { &*std::ptr::from_ref(value) }
}

impl SchemaStorage {
    fn parse(blob: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut seen = HashSet::new();
        let mut parsed = Vec::new();
        while cursor < blob.len() {
            let kind = *blob.get(cursor)?;
            let name = read_str(blob, cursor + 1)?;
            cursor += 2 + name.len();
            if name.is_empty() || !seen.insert(name) {
                return None;
            }
            let strategy = match kind {
                ATTRIBUTE_KIND_NUMBER => ParsedStrategy::Number,
                ATTRIBUTE_KIND_UINT64 => ParsedStrategy::Uint64,
                ATTRIBUTE_KIND_BOOLEAN => ParsedStrategy::Boolean,
                ATTRIBUTE_KIND_TEXT => ParsedStrategy::Text,
                ATTRIBUTE_KIND_ENUM => {
                    let count = u16::from_le_bytes([*blob.get(cursor)?, *blob.get(cursor + 1)?]);
                    cursor += 2;
                    if count == 0 {
                        return None;
                    }
                    let mut variants = Vec::with_capacity(usize::from(count));
                    let mut distinct = HashSet::with_capacity(usize::from(count));
                    for _ in 0..count {
                        let variant = read_str(blob, cursor)?;
                        cursor += 1 + variant.len();
                        if variant.is_empty() || !distinct.insert(variant) {
                            return None;
                        }
                        variants.push(Box::from(variant));
                    }
                    ParsedStrategy::Enum(variants)
                }
                _ => return None,
            };
            parsed.push((Box::<str>::from(name), strategy));
        }
        u16::try_from(parsed.len() + SYSTEM_COLUMN_COUNT).ok()?;

        let mut names = Vec::with_capacity(parsed.len());
        let mut variants = Vec::new();
        let mut variant_tables = Vec::new();
        let mut fields = Vec::with_capacity(parsed.len());
        for (name, strategy) in parsed {
            // SAFETY: `name`'s heap bytes move into `names`, which this storage
            // keeps; moving the `Box` does not move its referent.
            let name_ref = unsafe { extend(&*name) };
            names.push(name);
            let strategy = match strategy {
                ParsedStrategy::Number => FieldStrategy::Number,
                ParsedStrategy::Uint64 => FieldStrategy::Uint64,
                ParsedStrategy::Boolean => FieldStrategy::Boolean,
                ParsedStrategy::Text => FieldStrategy::Text,
                ParsedStrategy::Enum(owned) => {
                    // SAFETY: as above, for each variant's bytes and the table.
                    let table: Box<[&'static str]> = owned
                        .iter()
                        .map(|value| unsafe { extend(&**value) })
                        .collect();
                    let table_ref = unsafe { extend(&*table) };
                    variants.push(owned);
                    variant_tables.push(table);
                    FieldStrategy::Enum(table_ref)
                }
            };
            fields.push(FieldMeta::new(name_ref, strategy));
        }
        Some(Self {
            fields: fields.into_boxed_slice(),
            _names: names,
            _variants: variants,
            _variant_tables: variant_tables,
        })
    }
}

/// `[len:u8][bytes]` at `at`, as UTF-8.
fn read_str(blob: &[u8], at: usize) -> Option<&str> {
    let len = usize::from(*blob.get(at)?);
    std::str::from_utf8(blob.get(at + 1..at + 1 + len)?).ok()
}

/// A row store and the schema its columns were laid out from.
pub struct ThreadStore {
    buffer: ManuallyDrop<ThreadSpanBuffer>,
    schema: Option<SchemaStorage>,
}

impl ThreadStore {
    /// A store over generated, `'static` schema metadata.
    #[must_use]
    pub fn with_fields(thread_id: u64, capacity: usize, fields: &'static [FieldMeta]) -> Self {
        Self {
            buffer: ManuallyDrop::new(ThreadSpanBuffer::new(thread_id, capacity, fields)),
            schema: None,
        }
    }

    /// A store over a schema described by a blob, or `None` for a malformed
    /// blob. `capacity` must be a power of two within the tuning bounds.
    #[must_use]
    pub fn from_schema_blob(thread_id: u64, capacity: usize, blob: &[u8]) -> Option<Self> {
        if !capacity.is_power_of_two()
            || !(crate::MIN_CAPACITY..=crate::MAX_CAPACITY).contains(&capacity)
        {
            return None;
        }
        let schema = SchemaStorage::parse(blob)?;
        // SAFETY: `schema` moves into the returned store, which drops the
        // buffer before it (see `Drop`); the boxed slice's heap does not move.
        let fields = unsafe { extend(&*schema.fields) };
        Some(Self {
            buffer: ManuallyDrop::new(ThreadSpanBuffer::new(thread_id, capacity, fields)),
            schema: Some(schema),
        })
    }
}

impl std::fmt::Debug for ThreadStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadStore")
            .field("buffer", &*self.buffer)
            .field("owns_schema", &self.schema.is_some())
            .finish()
    }
}

impl Deref for ThreadStore {
    type Target = ThreadSpanBuffer;
    fn deref(&self) -> &ThreadSpanBuffer {
        &self.buffer
    }
}

impl DerefMut for ThreadStore {
    fn deref_mut(&mut self) -> &mut ThreadSpanBuffer {
        &mut self.buffer
    }
}

impl Drop for ThreadStore {
    fn drop(&mut self) {
        // SAFETY: dropped exactly once, here, before `schema` — the owner of
        // the metadata the buffer borrows — is dropped by field order.
        unsafe { ManuallyDrop::drop(&mut self.buffer) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(kind: u8, name: &str) -> Vec<u8> {
        let mut bytes = vec![kind, u8::try_from(name.len()).unwrap()];
        bytes.extend_from_slice(name.as_bytes());
        bytes
    }

    #[test]
    fn a_blob_schema_lays_out_its_fields_in_order() {
        let mut blob = field(ATTRIBUTE_KIND_NUMBER, "count");
        blob.extend(field(ATTRIBUTE_KIND_ENUM, "phase"));
        blob.extend([2, 0, 1, b'a', 1, b'b']);
        let store = ThreadStore::from_schema_blob(9, 8, &blob).unwrap();
        let fields = store.schema_fields();
        assert_eq!(fields[0].name, "count");
        assert_eq!(fields[1].strategy, FieldStrategy::Enum(&["a", "b"]));
    }

    #[test]
    fn a_malformed_blob_is_refused() {
        let duplicate = [
            field(ATTRIBUTE_KIND_TEXT, "x"),
            field(ATTRIBUTE_KIND_TEXT, "x"),
        ]
        .concat();
        assert!(ThreadStore::from_schema_blob(9, 8, &duplicate).is_none());
        assert!(ThreadStore::from_schema_blob(9, 8, &[ATTRIBUTE_KIND_TEXT, 4, b'a']).is_none());
        assert!(ThreadStore::from_schema_blob(9, 8, &[99, 1, b'a']).is_none());
        assert!(ThreadStore::from_schema_blob(9, 7, &[]).is_none());
    }
}
