//! Lazy attribute columns, per `specs/lmao/01b1_buffer_performance_optimizations.md`.
//!
//! System columns are eager (see [`crate::buffer::SpanBuffer`]); every schema
//! attribute column is lazy: zero bytes until the first write, then one
//! fixed-capacity allocation for the buffer's lifetime. Capacities are the
//! buffer's (power of two, so the null bitmap's byte-boundary requirement from
//! `01b1` holds automatically).
//!
//! String strategies from `01a_trace_schema_system.md` — NO hot-path interning:
//! - `enum`   → [`NumColumn<u16>`] index into a schema-time dictionary (zero flush work)
//! - `category`/`text` → [`StrColumn`]: raw `Arc<str>` slot writes; sort/dedupe and
//!   UTF-8 dictionary building are deferred to the Arrow flush pass (`lmao-arrow`).
//!
//! Deviation from the JS/WASM layout, documented on purpose: the spec bundles
//! null-bitmap + values into ONE ArrayBuffer/arena block. Here validity and values
//! are two boxed slices inside one lazily boxed struct (2 allocations at first
//! touch, 0 afterwards). The single-block bundling is an arena concern and lives in
//! `lmao-arena`; keeping `lmao-core` in safe Rust is worth the extra warmup alloc.

use std::sync::Arc;

/// A shared string slot value: `'static` borrows (log templates, compile-time
/// names) cost ZERO allocations; dynamic values ride an `Arc` refcount bump.
/// This is what keeps the zero-alloc gate honest for `log(template)`.
#[derive(Debug, Clone)]
pub enum SharedStr {
    Static(&'static str),
    Owned(Arc<str>),
}

impl SharedStr {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Static(s) => s,
            Self::Owned(s) => s,
        }
    }
}

/// Flush strategy preserved by schema code generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldStrategy {
    Number,
    Uint64,
    Boolean,
    Category,
    Text,
    Enum(&'static [&'static str]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldMeta {
    pub name: &'static str,
    pub strategy: FieldStrategy,
}

impl FieldMeta {
    pub const fn new(name: &'static str, strategy: FieldStrategy) -> Self {
        Self { name, strategy }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnumIndexError {
    pub field: &'static str,
    pub index: u16,
    pub variants: u16,
}

impl std::fmt::Display for EnumIndexError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "enum field {} index {} is outside 0..{}",
            self.field, self.index, self.variants
        )
    }
}

impl std::error::Error for EnumIndexError {}

impl From<&'static str> for SharedStr {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self::Static(s)
    }
}

impl From<Arc<str>> for SharedStr {
    #[inline]
    fn from(s: Arc<str>) -> Self {
        Self::Owned(s)
    }
}

impl From<String> for SharedStr {
    fn from(s: String) -> Self {
        Self::Owned(s.into())
    }
}

impl PartialEq for SharedStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for SharedStr {}

/// Fixed-capacity validity bitmap + values, allocated at first touch.
#[derive(Debug)]
struct ColumnBuf<T> {
    validity: Box<[u8]>,
    values: Box<[T]>,
}

impl<T: Copy + Default> ColumnBuf<T> {
    fn new(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        Self {
            validity: vec![0u8; capacity / 8].into_boxed_slice(),
            values: vec![T::default(); capacity].into_boxed_slice(),
        }
    }
}

/// Lazy numeric column (also carries `bool` and enum-index `u16` values).
#[derive(Debug, Default)]
pub struct NumColumn<T> {
    buf: Option<Box<ColumnBuf<T>>>,
}

impl<T: Copy + Default> NumColumn<T> {
    pub const fn new() -> Self {
        Self { buf: None }
    }

    /// Write `value` at `row`, allocating at `capacity` on first touch.
    /// After first touch this is two stores (bitmap bit + value) — the hot path.
    #[inline]
    pub fn set(&mut self, row: usize, capacity: usize, value: T) {
        let buf = self
            .buf
            .get_or_insert_with(|| Box::new(ColumnBuf::new(capacity)));
        buf.validity[row >> 3] |= 1 << (row & 7);
        buf.values[row] = value;
    }

    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        self.buf
            .as_ref()
            .is_some_and(|b| b.validity[row >> 3] & (1 << (row & 7)) != 0)
    }

    #[inline]
    pub fn get(&self, row: usize) -> Option<T> {
        self.is_valid(row)
            .then(|| self.buf.as_ref().unwrap().values[row])
    }

    #[inline]
    pub fn is_allocated(&self) -> bool {
        self.buf.is_some()
    }

    /// Heap bytes owned by this column (0 when never touched) — drives the
    /// lazy-memory-accounting property tests.
    pub fn allocated_bytes(&self) -> usize {
        self.buf
            .as_ref()
            .map(|b| b.validity.len() + b.values.len() * size_of::<T>())
            .unwrap_or(0)
    }

    /// Raw view for the flush pass: `(validity_bitmap, values)`.
    pub fn raw(&self) -> Option<(&[u8], &[T])> {
        self.buf.as_ref().map(|b| (&*b.validity, &*b.values))
    }

    /// Fill every row in `0..rows` that has NO direct write with `value`, and
    /// return how many rows were filled. This is the cold-path scope
    /// materialization of `01i`: *direct writes always win*, scope only fills the
    /// cells a direct write left null.
    ///
    /// Allocates at `capacity` if the column was never touched, because a column
    /// with no direct writes at all is exactly the case where scope supplies every
    /// value.
    ///
    /// The scan walks the validity bitmap a BYTE at a time, which is where `01i`'s
    /// "SIMD where possible" actually lands: a `0x00` byte is eight unwritten rows
    /// that become one vectorizable `fill` of eight values, and `0xFF` is eight rows
    /// skipped by a single compare. Only a byte with mixed validity pays per-bit
    /// cost. Scope is normally set for a whole span and direct writes are sparse, so
    /// the all-zero byte is the common case by a wide margin.
    pub fn fill_unset(&mut self, rows: usize, capacity: usize, value: T) -> usize {
        if rows == 0 {
            return 0;
        }
        let buf = self
            .buf
            .get_or_insert_with(|| Box::new(ColumnBuf::new(capacity)));
        debug_assert!(
            rows <= buf.values.len(),
            "fill range exceeds column capacity"
        );

        let mut filled = 0usize;
        let whole_bytes = rows >> 3;
        for byte in 0..whole_bytes {
            let validity = buf.validity[byte];
            if validity == u8::MAX {
                continue;
            }
            let base = byte << 3;
            if validity == 0 {
                buf.values[base..base + 8].fill(value);
                filled += 8;
            } else {
                for bit in 0..8 {
                    if validity & (1 << bit) == 0 {
                        buf.values[base + bit] = value;
                        filled += 1;
                    }
                }
            }
            buf.validity[byte] = u8::MAX;
        }

        let tail = rows & 7;
        if tail != 0 {
            let base = whole_bytes << 3;
            let validity = buf.validity[whole_bytes];
            for bit in 0..tail {
                if validity & (1 << bit) == 0 {
                    buf.values[base + bit] = value;
                    buf.validity[whole_bytes] |= 1 << bit;
                    filled += 1;
                }
            }
        }
        filled
    }
}

pub type F64Column = NumColumn<f64>;
pub type U64Column = NumColumn<u64>;
pub type BoolColumn = NumColumn<bool>;
/// Enum strategy: u16 index into a schema-time `&'static [&'static str]` dictionary.
pub type EnumColumn = NumColumn<u16>;

/// Lazy string column for `category`/`text` fields: raw shared-slot writes,
/// `None` = null (no separate bitmap needed, matching the JS `undefined`=null
/// convention). Slots hold [`SharedStr`]: `'static` templates are free, dynamic
/// values are a refcount bump — either way the post-warmup path satisfies the
/// zero-alloc gate.
#[derive(Debug, Default)]
pub struct StrColumn {
    buf: Option<Box<[Option<SharedStr>]>>,
}

impl StrColumn {
    pub const fn new() -> Self {
        Self { buf: None }
    }

    #[inline]
    pub fn set(&mut self, row: usize, capacity: usize, value: impl Into<SharedStr>) {
        let buf = self
            .buf
            .get_or_insert_with(|| vec![None; capacity].into_boxed_slice());
        buf[row] = Some(value.into());
    }

    #[inline]
    pub fn get(&self, row: usize) -> Option<&str> {
        Some(self.buf.as_ref()?.get(row)?.as_ref()?.as_str())
    }

    #[inline]
    pub fn is_allocated(&self) -> bool {
        self.buf.is_some()
    }

    pub fn allocated_bytes(&self) -> usize {
        self.buf
            .as_ref()
            .map(|b| b.len() * size_of::<Option<SharedStr>>())
            .unwrap_or(0)
    }

    /// Raw slot view for the flush pass.
    pub fn raw(&self) -> Option<&[Option<SharedStr>]> {
        self.buf.as_deref()
    }

    /// Fill every null slot in `0..rows` with `value`, returning how many were
    /// filled — the [`NumColumn::fill_unset`] counterpart for string columns.
    /// `None` IS the null here, so there is no bitmap to consult and the slot's own
    /// emptiness is the authority on whether a direct write happened.
    pub fn fill_unset(&mut self, rows: usize, capacity: usize, value: &SharedStr) -> usize {
        if rows == 0 {
            return 0;
        }
        let buf = self
            .buf
            .get_or_insert_with(|| vec![None; capacity].into_boxed_slice());
        debug_assert!(rows <= buf.len(), "fill range exceeds column capacity");
        let mut filled = 0usize;
        for slot in buf[..rows].iter_mut() {
            if slot.is_none() {
                // Static values copy a pointer pair; dynamic ones bump an Arc.
                *slot = Some(value.clone());
                filled += 1;
            }
        }
        filled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_columns_cost_zero_until_touched() {
        let col = F64Column::new();
        assert!(!col.is_allocated());
        assert_eq!(col.allocated_bytes(), 0);
        let mut col = col;
        col.set(3, 64, 1.5);
        assert!(col.is_allocated());
        assert_eq!(col.allocated_bytes(), 64 / 8 + 64 * 8);
        assert_eq!(col.get(3), Some(1.5));
        assert_eq!(col.get(4), None);
    }

    #[test]
    fn str_column_null_is_absence() {
        let mut col = StrColumn::new();
        assert_eq!(col.allocated_bytes(), 0);
        col.set(2, 8, "hello");
        assert_eq!(col.get(2), Some("hello"));
        assert_eq!(col.get(1), None);
    }

    /// `01i`: direct writes win, scope fills only the cells they left null. The
    /// range crosses the byte boundary at row 8 deliberately, so the whole-byte
    /// path, the mixed-byte path and the tail path are all exercised.
    #[test]
    fn fill_unset_preserves_direct_writes() {
        let mut col = F64Column::new();
        col.set(0, 64, 1.0);
        col.set(9, 64, 2.0);

        assert_eq!(
            col.fill_unset(13, 64, 9.9),
            11,
            "13 rows minus 2 direct writes"
        );

        assert_eq!(col.get(0), Some(1.0), "direct write survives");
        assert_eq!(col.get(9), Some(2.0), "direct write survives across a byte");
        for row in [1, 7, 8, 10, 12] {
            assert_eq!(col.get(row), Some(9.9), "row {row} filled from scope");
        }
        assert_eq!(col.get(13), None, "fill stops at the row count");
    }

    /// A column with no direct writes at all is the case where scope supplies every
    /// value, so the fill must allocate rather than silently drop the values.
    #[test]
    fn fill_unset_allocates_an_untouched_column() {
        let mut col = U64Column::new();
        assert!(!col.is_allocated());
        assert_eq!(col.fill_unset(8, 32, 7), 8);
        assert!(col.is_allocated());
        assert_eq!(col.get(0), Some(7));
        assert_eq!(col.get(7), Some(7));
        assert_eq!(col.get(8), None);
    }

    #[test]
    fn fill_unset_is_idempotent_and_zero_rows_is_a_no_op() {
        let mut col = F64Column::new();
        assert_eq!(col.fill_unset(0, 64, 1.0), 0);
        assert!(
            !col.is_allocated(),
            "an empty range must not force allocation"
        );

        assert_eq!(col.fill_unset(4, 64, 1.0), 4);
        assert_eq!(
            col.fill_unset(4, 64, 2.0),
            0,
            "already-filled rows are direct writes now"
        );
        assert_eq!(col.get(0), Some(1.0));
    }

    #[test]
    fn str_fill_unset_preserves_direct_writes() {
        let mut col = StrColumn::new();
        col.set(1, 8, "direct");
        assert_eq!(col.fill_unset(4, 8, &SharedStr::Static("scope")), 3);
        assert_eq!(col.get(0), Some("scope"));
        assert_eq!(col.get(1), Some("direct"));
        assert_eq!(col.get(3), Some("scope"));
        assert_eq!(col.get(4), None);
    }
}
