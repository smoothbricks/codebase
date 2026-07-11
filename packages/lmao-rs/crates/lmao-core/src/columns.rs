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
/// This is what keeps the AxE zero-alloc gate honest for `log(template)`.
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
/// AxE zero-alloc gate.
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
}
