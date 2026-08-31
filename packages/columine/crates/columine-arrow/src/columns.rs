//! Columnar buffers for parsed events.
//!
//! One store, [`DynamicColumns`], for every schema — including the four-column
//! event log, which used to have a second hand-written store of its own that
//! could not grow past its per-event byte estimates.
//!
//! Arrow compatibility:
//! - String columns use offset/length encoding: `offsets[i]` = start of
//!   string `i`, `offsets[count]` = total data length (n+1 offsets).
//! - Null bitmaps use Arrow's LSB-first bit packing.
//!
//! Storage is little-endian byte vectors (crate-family convention): the IPC
//! writer borrows `&[u8]` views of offsets/fixed-width data with no copy and
//! no `unsafe`.

/// Maximum events per batch (prevents unbounded growth).
pub const MAX_EVENTS_PER_BATCH: u32 = 65536;

/// Maximum bytes for a single string column (id, type).
pub const MAX_STRING_BYTES: u32 = 1024 * 1024; // 1MB

/// Maximum bytes for a value column (serialized JSON/msgpack).
pub const MAX_VALUE_BYTES: u32 = 16 * 1024 * 1024; // 16MB

/// TypeScript `EventLogError` discriminants (the JS interop contract), pinned
/// by `parse_error_codes_match_ts`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum ParseError {
    Ok = 0,
    InvalidJson = 1,
    InvalidMsgpack = 2,
    MissingField = 3,
    InvalidFieldType = 4,
    TooManyEvents = 5,
    BufferOverflow = 6,
    OutOfMemory = 7,
}

/// Read offset entry `index` from an offsets buffer of `width`-byte entries.
///
/// Retained variable-width data is capped at [`MAX_VALUE_BYTES`], so a 64-bit
/// offset never uses more than its low four little-endian bytes.
fn read_offset(bytes: &[u8], index: usize, width: u32) -> u32 {
    let start = index * width as usize;
    let mut value = [0u8; 4];
    value.copy_from_slice(&bytes[start..start + 4]);
    u32::from_le_bytes(value)
}

/// Write offset entry `index`. The high four bytes of a 64-bit offset are left
/// alone: the buffer is allocated zeroed and nothing ever writes a value that
/// reaches them.
fn write_offset(bytes: &mut [u8], index: usize, width: u32, value: u32) {
    let start = index * width as usize;
    bytes[start..start + 4].copy_from_slice(&value.to_le_bytes());
}

/// IEEE-754 `binary32` to `binary16`, round to nearest with ties to even.
///
/// Written out instead of pulling in `half`: this crate compiles to wasm and
/// the whole conversion is bit shuffling, so the dependency would buy nothing.
fn f16_bits_from_f32(value: f32) -> u16 {
    let bits = value.to_bits();
    let sign = ((bits >> 16) as u16) & 0x8000;
    let exponent = ((bits >> 23) & 0xff) as i32;
    let mantissa = bits & 0x007f_ffff;

    if exponent == 0xff {
        // Infinity keeps an empty payload; a NaN must keep a nonzero one or it
        // would round into infinity, changing the value and not its precision.
        return sign | 0x7c00 | u16::from(mantissa != 0);
    }

    // A `binary32` subnormal (exponent 0) is smaller than 2^-126, far below
    // the smallest `binary16` subnormal at 2^-24, so it always rounds to zero
    // through the `extra > 24` arm below and never reaches the code that
    // assumes an implicit leading one.
    let exponent16 = exponent - 127 + 15;
    if exponent16 >= 0x1f {
        return sign | 0x7c00; // overflows `binary16`: saturate to infinity
    }
    let significand = mantissa | 0x0080_0000;
    let (exponent_field, drop) = if exponent16 >= 1 {
        (exponent16 as u32, 13u32)
    } else {
        // Below the smallest normal, `binary16` trades exponent range for
        // significand bits by dropping more of them.
        let extra = 1 - exponent16;
        if extra > 24 {
            return sign;
        }
        (0, 13 + extra as u32)
    };

    let kept = significand >> drop;
    let remainder = significand & ((1u32 << drop) - 1);
    let halfway = 1u32 << (drop - 1);
    let round_up = remainder > halfway || (remainder == halfway && kept & 1 == 1);
    // A carry out of the ten stored significand bits lands in the exponent
    // field, which is exactly the increment the rounded value needs.
    let packed = ((exponent_field << 10) | (kept & 0x03ff)) + u32::from(round_up);
    sign | packed as u16
}

/// IEEE-754 `binary16` bits to `binary32`. Exact: every half is a float.
fn f32_from_f16_bits(bits: u16) -> f32 {
    let sign = u32::from(bits & 0x8000) << 16;
    let exponent = i32::from((bits >> 10) & 0x1f);
    let mantissa = u32::from(bits & 0x03ff);
    if exponent == 0x1f {
        return f32::from_bits(sign | 0x7f80_0000 | (mantissa << 13));
    }
    if exponent == 0 {
        if mantissa == 0 {
            return f32::from_bits(sign);
        }
        // Subnormal half: shift the leading one up into the implicit position
        // and pay for it out of the exponent.
        let shift = mantissa.leading_zeros() - 21;
        let exponent32 = (127 - 15 - shift + 1) << 23;
        return f32::from_bits(sign | exponent32 | ((mantissa << (13 + shift)) & 0x007f_ffff));
    }
    f32::from_bits(sign | (((exponent - 15 + 127) as u32) << 23) | (mantissa << 13))
}

/// Store the low `width` little-endian bytes of a fixed-width cell and mark it
/// valid. Callers range-check the value against `width` first, so the low bytes
/// are the whole value and not a truncation of it.
fn write_cell(column: &mut ColumnStorage, row_idx: usize, width: u32, value: [u8; 8]) {
    let start = row_idx * width as usize;
    column.values[start..start + width as usize].copy_from_slice(&value[..width as usize]);
    column.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
}

/// Storage for a single typed column. All columns are nullable, so a validity
/// bitmap is always present.
///
/// Two byte buffers, not five: `offsets` exists only for the variable-width
/// kinds, and `values` holds whatever one row of the plane is — little-endian
/// fixed-width values, Arrow LSB-first bits, or the variable-width payload.
/// The plane kind says which, so no access needs a `die!` to assert that this
/// shape really does have that buffer.
#[derive(Clone, Debug)]
pub struct ColumnStorage {
    pub kind: PlaneKind,

    /// Validity bitmap (Arrow LSB-first).
    validity: Vec<u8>,

    /// Monotone offsets, `offset_width` bytes per entry; empty for the
    /// fixed-width kinds.
    offsets: Vec<u8>,

    /// Fixed-width values, packed bits, or variable-width payload.
    values: Vec<u8>,

    /// Bytes of `values` in use. Meaningful for the variable-width kinds only;
    /// every other kind addresses `values` by row.
    pub data_len: u32,

    /// Configured hard limit for retained variable-width data.
    max_data_bytes: u32,
}

impl ColumnStorage {
    pub fn new(kind: PlaneKind, capacity: u32) -> Self {
        Self::with_variable_limit(kind, capacity, MAX_VALUE_BYTES)
    }

    /// Storage is sized by the plane's ACTUAL per-row cost:
    ///
    /// - fixed-width kinds: `capacity * value_width`, exactly. An i8 column
    ///   reserves one byte per row, not the 128 a variable-width column
    ///   reserves, so a narrow plane is the cheapest column in a schema rather
    ///   than tied for the most expensive.
    /// - `Bool`: one bit per row.
    /// - variable-width kinds: `(capacity + 1) * offset_width` for offsets and
    ///   `min(max_data_bytes, capacity * 128)` for the payload, growing
    ///   geometrically from there. The 128 is an estimate, and this is the only
    ///   kind that needs one, because it is the only kind whose per-row cost
    ///   the schema does not state.
    /// - `Empty`: nothing at all.
    pub fn with_variable_limit(kind: PlaneKind, capacity: u32, max_data_bytes: u32) -> Self {
        let cap = capacity.min(MAX_EVENTS_PER_BATCH) as usize;
        let (offsets, values) = match kind {
            PlaneKind::Empty => (Vec::new(), Vec::new()),
            PlaneKind::Bool => (Vec::new(), vec![0; cap.div_ceil(8)]),
            PlaneKind::SignedInt { width }
            | PlaneKind::UnsignedInt { width }
            | PlaneKind::Float { width }
            | PlaneKind::FixedBytes { width } => (Vec::new(), vec![0; cap * width as usize]),
            PlaneKind::Text { offset_width } | PlaneKind::Bytes { offset_width } => (
                vec![0; (cap + 1) * offset_width as usize],
                vec![0; (max_data_bytes as usize).min(cap * 128)],
            ),
        };
        Self {
            kind,
            validity: vec![0; cap.div_ceil(8)],
            offsets,
            values,
            data_len: 0,
            max_data_bytes,
        }
    }

    /// Reset for reuse. Grown variable-width allocations are retained rather
    /// than reallocated.
    pub fn reset(&mut self) {
        self.validity.fill(0);
        self.data_len = 0;
        // Only the bit-packed kind needs clearing: `append_bool` sets bits and
        // never clears them, while every other kind overwrites the whole cell.
        if self.kind == PlaneKind::Bool {
            self.values.fill(0);
        }
    }

    /// Grow the variable-width payload geometrically, capped at
    /// `max_data_bytes`. Only `preserve_end` bytes survive reallocation; bytes
    /// past it are scratch.
    fn ensure_variable_capacity_preserving(
        &mut self,
        required: usize,
        preserve_end: usize,
    ) -> Result<(), ParseError> {
        if self.kind.offset_width().is_none() {
            return Err(ParseError::InvalidFieldType);
        }
        if required <= self.values.len() {
            return Ok(());
        }
        if required > self.max_data_bytes as usize {
            return Err(ParseError::BufferOverflow);
        }

        let max = self.max_data_bytes as usize;
        let mut new_capacity = self.values.len();
        while new_capacity < required {
            if new_capacity >= max {
                return Err(ParseError::BufferOverflow);
            }
            new_capacity = if new_capacity > max / 2 {
                max
            } else {
                (new_capacity * 2).max(1)
            };
        }

        let mut replacement = vec![0u8; new_capacity];
        debug_assert!(preserve_end <= self.values.len());
        replacement[..preserve_end].copy_from_slice(&self.values[..preserve_end]);
        self.values = replacement;
        Ok(())
    }

    fn ensure_variable_capacity(&mut self, required: usize) -> Result<(), ParseError> {
        self.ensure_variable_capacity_preserving(required, self.data_len as usize)
    }

    pub fn validity_bytes(&self, row_count: u32) -> &[u8] {
        &self.validity[..(row_count as usize).div_ceil(8)]
    }

    /// The offsets buffer for the variable-width kinds, `None` otherwise.
    pub fn offsets_bytes(&self, row_count: u32) -> Option<&[u8]> {
        let width = self.kind.offset_width()? as usize;
        Some(&self.offsets[..(row_count as usize + 1) * width])
    }

    /// The plane's data buffer at `row_count` rows: fixed-width values, packed
    /// bits, or the used prefix of the variable-width payload. One accessor
    /// for every plane, because the Arrow body wants the same thing from all
    /// of them — the bytes that back these rows.
    pub fn value_bytes(&self, row_count: u32) -> &[u8] {
        let rows = row_count as usize;
        match self.kind {
            PlaneKind::Empty => &[],
            PlaneKind::Bool => &self.values[..rows.div_ceil(8)],
            PlaneKind::SignedInt { width }
            | PlaneKind::UnsignedInt { width }
            | PlaneKind::Float { width }
            | PlaneKind::FixedBytes { width } => &self.values[..rows * width as usize],
            PlaneKind::Text { .. } | PlaneKind::Bytes { .. } => {
                &self.values[..self.data_len as usize]
            }
        }
    }

    /// Full retained payload capacity, useful for checking warm reuse.
    pub fn data_capacity(&self) -> usize {
        self.values.len()
    }

    /// The plane's `width` value bytes for `row`, little-endian, zero-extended
    /// into a `u64`. Only for widths up to eight.
    fn raw_value(&self, row: u32, width: u32) -> Option<u64> {
        let start = row as usize * width as usize;
        let cell = self.values.get(start..start + width as usize)?;
        let mut bytes = [0u8; 8];
        bytes.get_mut(..cell.len())?.copy_from_slice(cell);
        Some(u64::from_le_bytes(bytes))
    }

    /// Read a signed cell, sign-extending from the plane's width.
    ///
    /// Sign extension is the whole reason signed and unsigned are separate
    /// planes: a four-byte cell holding `0xFFFF_FFFF` is -1 here and
    /// 4294967295 in [`ColumnStorage::read_uint`], and no width check can tell
    /// those two apart.
    pub fn read_int(&self, row: u32) -> Option<i64> {
        let PlaneKind::SignedInt { width } = self.kind else {
            return None;
        };
        let raw = self.raw_value(row, width)?;
        let shift = 64 - width * 8;
        Some(((raw << shift) as i64) >> shift)
    }

    /// Read an unsigned cell, zero-extending from the plane's width.
    pub fn read_uint(&self, row: u32) -> Option<u64> {
        let PlaneKind::UnsignedInt { width } = self.kind else {
            return None;
        };
        self.raw_value(row, width)
    }

    /// Read a float cell, widening from the plane's width. `binary16` is
    /// decoded from its raw bits because stable Rust has no `f16`.
    pub fn read_float(&self, row: u32) -> Option<f64> {
        let PlaneKind::Float { width } = self.kind else {
            return None;
        };
        let raw = self.raw_value(row, width)?;
        Some(match width {
            2 => f64::from(f32_from_f16_bits(raw as u16)),
            4 => f64::from(f32::from_bits(raw as u32)),
            8 => f64::from_bits(raw),
            // `plane_kind` only ever builds Float with width 2, 4 or 8.
            _ => return None,
        })
    }

    pub fn read_bool(&self, row: u32) -> Option<bool> {
        if self.kind != PlaneKind::Bool {
            return None;
        }
        let byte = self.values.get(row as usize / 8)?;
        Some((byte & (1u8 << (row as usize % 8))) != 0)
    }

    /// Read an opaque fixed-size cell: a decimal, a wide interval, or a
    /// fixed-size binary value. The plane does not interpret the bytes.
    pub fn read_fixed_bytes(&self, row: u32) -> Option<&[u8]> {
        let PlaneKind::FixedBytes { width } = self.kind else {
            return None;
        };
        let start = row as usize * width as usize;
        self.values.get(start..start + width as usize)
    }

    pub fn read_variable(&self, row: u32) -> Option<&[u8]> {
        let width = self.kind.offset_width()?;
        let start = read_offset(&self.offsets, row as usize, width) as usize;
        let end = read_offset(&self.offsets, row as usize + 1, width) as usize;
        self.values.get(start..end)
    }
}


/// Errors surfaced by the transactional variable-width writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VariableValueError {
    InvalidFieldType,
    BufferOverflow,
    OutOfMemory,
}

fn variable_error(code: ParseError) -> VariableValueError {
    match code {
        ParseError::BufferOverflow => VariableValueError::BufferOverflow,
        ParseError::OutOfMemory => VariableValueError::OutOfMemory,
        _ => VariableValueError::InvalidFieldType,
    }
}

/// Transactional writer for one variable-width cell: capacity growth is
/// retained, but offset, length, and validity are published only by `commit`,
/// so a parser failure leaves the column logically unchanged.
///
/// The reservation identifies the column by index and borrows the parent only
/// for each operation, preventing aliasing.
#[derive(Clone, Copy, Debug)]
pub struct VariableValueReservation {
    pub col_idx: u32,
    pub row_index: u32,
    pub start: u32,
}

impl VariableValueReservation {
    pub fn ensure_capacity(
        &self,
        cols: &mut DynamicColumns,
        relative_len: usize,
    ) -> Result<(), VariableValueError> {
        self.ensure_capacity_preserving(cols, relative_len, relative_len)
    }

    pub fn ensure_capacity_preserving(
        &self,
        cols: &mut DynamicColumns,
        relative_len: usize,
        preserve_relative_len: usize,
    ) -> Result<(), VariableValueError> {
        let required = (self.start as usize)
            .checked_add(relative_len)
            .ok_or(VariableValueError::BufferOverflow)?;
        let preserve_end = (self.start as usize)
            .checked_add(preserve_relative_len)
            .ok_or(VariableValueError::BufferOverflow)?;
        cols.columns[self.col_idx as usize]
            .ensure_variable_capacity_preserving(required, preserve_end)
            .map_err(variable_error)
    }

    /// The writable tail of the column's payload buffer, starting at the
    /// reservation. Empty when the column is not variable-width, which
    /// `reserve_binary_value` has already ruled out.
    pub fn buffer<'a>(&self, cols: &'a mut DynamicColumns) -> &'a mut [u8] {
        let column = &mut cols.columns[self.col_idx as usize];
        &mut column.values[self.start as usize..]
    }

    /// Publish the cell's offset, data length, and validity after a
    /// successful transactional write.
    pub fn commit(
        &self,
        cols: &mut DynamicColumns,
        relative_len: usize,
    ) -> Result<(), VariableValueError> {
        self.ensure_capacity(cols, relative_len)?;
        let end = (self.start as usize)
            .checked_add(relative_len)
            .ok_or(VariableValueError::BufferOverflow)?;
        let column = &mut cols.columns[self.col_idx as usize];
        let offset_width = column
            .kind
            .offset_width()
            .ok_or(VariableValueError::InvalidFieldType)?;
        let row = self.row_index as usize;
        write_offset(&mut column.offsets, row, offset_width, self.start);
        column.data_len = end as u32;
        column.validity[row / 8] |= 1u8 << (row % 8);
        Ok(())
    }
}

/// Dynamic columnar buffers for N-column extraction. All value.* columns are
/// nullable because events may omit declared fields (sparse data).
#[derive(Clone, Debug)]
pub struct DynamicColumns {
    /// Number of rows.
    pub count: u32,
    /// Per-column storage.
    pub columns: Vec<ColumnStorage>,
    /// Reused per-row presence workspace for schema-width extraction.
    pub columns_seen: Vec<bool>,
    /// Field count (matches schema).
    pub field_count: u32,
    /// Column capacity (max rows).
    pub capacity: u32,
}

use crate::schema::{PlaneKind, SignalSchemaField};

impl DynamicColumns {
    /// Initialize from schema field metadata: every field's storage follows
    /// from its plane kind, including the byte width, so there is no per-type
    /// dispatch left to get wrong.
    pub fn new(field_metadata: &[SignalSchemaField], capacity: u32) -> Self {
        let cap = capacity.min(MAX_EVENTS_PER_BATCH);
        let columns = field_metadata
            .iter()
            // The Null plane allocates nothing: it has no buffers in the Arrow
            // body and every row of it is null. It used to be stored as an
            // empty utf8 column, paying for offsets and a payload no reader
            // could ever reach.
            .map(|field| ColumnStorage::new(field.plane_kind(), cap))
            .collect::<Vec<_>>();
        Self {
            count: 0,
            columns_seen: vec![false; field_metadata.len()],
            field_count: field_metadata.len() as u32,
            capacity: cap,
            columns,
        }
    }

    /// Reset all columns for reuse.
    pub fn reset(&mut self) {
        self.count = 0;
        self.columns_seen.fill(false);
        for col in &mut self.columns {
            col.reset();
        }
    }

    /// Begin a new row; false when at capacity. Appends target `self.count`;
    /// no row state is allocated until a value is written.
    pub fn begin_row(&mut self) -> bool {
        self.count < self.capacity
    }

    /// Complete the current row: bump the count and publish the n+1 offset
    /// for every variable-length column.
    pub fn end_row(&mut self) {
        self.count += 1;
        let count = self.count as usize;
        for col in &mut self.columns {
            if let Some(width) = col.kind.offset_width() {
                let data_len = col.data_len;
                write_offset(&mut col.offsets, count, width, data_len);
            }
        }
    }

    /// Abandon the current row without bumping the count. Appended bytes and
    /// validity bits remain outside the logical row count, so failed extraction
    /// cannot publish a partial row.
    pub fn abandon_row(&mut self) {
        // Appends target row `count`; without end_row the row never becomes
        // visible. Validity bits set for the dead row are masked by count on
        // Every read path masks validity bits by the logical row count.
    }

    /// Reserve a transactional writer for a binary cell.
    pub fn reserve_binary_value(
        &mut self,
        col_idx: u32,
    ) -> Result<VariableValueReservation, VariableValueError> {
        if col_idx >= self.field_count {
            return Err(VariableValueError::InvalidFieldType);
        }
        let column = &self.columns[col_idx as usize];
        if !matches!(column.kind, PlaneKind::Bytes { .. }) {
            return Err(VariableValueError::InvalidFieldType);
        }
        Ok(VariableValueReservation {
            col_idx,
            row_index: self.count,
            start: column.data_len,
        })
    }

    /// Mutable storage for the cell being appended, with the row index.
    ///
    /// Every typed append needs the same three things — bounds check, current
    /// row, column — so they ask once here instead of restating it five times.
    fn cell(&mut self, col_idx: u32) -> Result<(usize, &mut ColumnStorage), ParseError> {
        let row_idx = self.count as usize;
        let column = self
            .columns
            .get_mut(col_idx as usize)
            .filter(|_| col_idx < self.field_count)
            .ok_or(ParseError::InvalidFieldType)?;
        Ok((row_idx, column))
    }

    /// Append a variable-width value: text or opaque bytes.
    ///
    /// UTF-8 validity is the producer's business here — the extractors hand
    /// over bytes they already decoded, and the compact path validates them at
    /// the boundary where they arrive from outside.
    ///
    /// Faithful append semantics: the row's offset is set to the CURRENT
    /// data_len and bytes are appended. A second append to the same cell in
    /// one row moves the offset forward and leaves the first bytes as dead
    /// data between offsets — observable in the IPC body byte image (pinned
    /// by `duplicate_append_leaves_dead_bytes`).
    pub fn append_variable(&mut self, col_idx: u32, value: &[u8]) -> Result<(), ParseError> {
        let (row_idx, col) = self.cell(col_idx)?;
        let Some(offset_width) = col.kind.offset_width() else {
            return Err(ParseError::InvalidFieldType);
        };
        let required = (col.data_len as usize)
            .checked_add(value.len())
            .ok_or(ParseError::BufferOverflow)?;
        col.ensure_variable_capacity(required)?;

        let data_len = col.data_len;
        write_offset(&mut col.offsets, row_idx, offset_width, data_len);
        col.values[data_len as usize..][..value.len()].copy_from_slice(value);
        col.data_len += value.len() as u32;
        col.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
        Ok(())
    }

    /// Append into a signed integer plane, range-checked to the plane's width.
    ///
    /// A value that does not fit is rejected, not truncated: storing 300 in an
    /// i8 column as 44 is a silently wrong number, and this crate does not
    /// produce those.
    pub fn append_int(&mut self, col_idx: u32, value: i64) -> Result<(), ParseError> {
        let (row_idx, col) = self.cell(col_idx)?;
        let PlaneKind::SignedInt { width } = col.kind else {
            return Err(ParseError::InvalidFieldType);
        };
        if !col.kind.holds_int(value) {
            return Err(ParseError::InvalidFieldType);
        }
        write_cell(col, row_idx, width, value.to_le_bytes());
        Ok(())
    }

    /// Append into an unsigned integer plane, range-checked to the plane's
    /// width. `0xFFFF_FFFF` in a four-byte unsigned plane is 4294967295, which
    /// is exactly the value the signed plane cannot hold.
    pub fn append_uint(&mut self, col_idx: u32, value: u64) -> Result<(), ParseError> {
        let (row_idx, col) = self.cell(col_idx)?;
        let PlaneKind::UnsignedInt { width } = col.kind else {
            return Err(ParseError::InvalidFieldType);
        };
        if !col.kind.holds_uint(value) {
            return Err(ParseError::InvalidFieldType);
        }
        write_cell(col, row_idx, width, value.to_le_bytes());
        Ok(())
    }

    /// Append into a float plane, narrowing to its width. `binary32` and
    /// `binary16` round to nearest with ties to even, like every other
    /// IEEE-754 narrowing.
    pub fn append_float(&mut self, col_idx: u32, value: f64) -> Result<(), ParseError> {
        let (row_idx, col) = self.cell(col_idx)?;
        let PlaneKind::Float { width } = col.kind else {
            return Err(ParseError::InvalidFieldType);
        };
        let raw = match width {
            2 => u64::from(f16_bits_from_f32(value as f32)),
            4 => u64::from((value as f32).to_bits()),
            8 => value.to_bits(),
            // `plane_kind` only ever builds Float with width 2, 4 or 8.
            _ => return Err(ParseError::InvalidFieldType),
        };
        write_cell(col, row_idx, width, raw.to_le_bytes());
        Ok(())
    }

    /// Append an opaque fixed-size value: a decimal, a wide interval, or a
    /// fixed-size binary cell.
    ///
    /// The slice must be exactly the plane's width. A short decimal is a
    /// different number, not a shorter one, so a length mismatch is an error
    /// rather than a zero-fill.
    pub fn append_fixed_bytes(&mut self, col_idx: u32, value: &[u8]) -> Result<(), ParseError> {
        let (row_idx, col) = self.cell(col_idx)?;
        let PlaneKind::FixedBytes { width } = col.kind else {
            return Err(ParseError::InvalidFieldType);
        };
        if value.len() != width as usize {
            return Err(ParseError::InvalidFieldType);
        }
        let start = row_idx * width as usize;
        col.values[start..start + value.len()].copy_from_slice(value);
        col.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
        Ok(())
    }

    /// Append a boolean value (Arrow LSB-first).
    pub fn append_bool(&mut self, col_idx: u32, value: bool) -> Result<(), ParseError> {
        let (row_idx, col) = self.cell(col_idx)?;
        if col.kind != PlaneKind::Bool {
            return Err(ParseError::InvalidFieldType);
        }
        if value {
            col.values[row_idx / 8] |= 1u8 << (row_idx % 8);
        }
        col.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
        Ok(())
    }

    /// Append null (no-op: null is the default validity state).
    pub fn append_null(&mut self, col_idx: u32) -> Result<(), ParseError> {
        if col_idx >= self.field_count {
            return Err(ParseError::InvalidFieldType);
        }
        Ok(())
    }

    /// True if the cell is null (out-of-range coordinates are null).
    pub fn is_null(&self, col_idx: u32, row_idx: u32) -> bool {
        if col_idx >= self.field_count || row_idx >= self.count {
            return true;
        }
        let col = &self.columns[col_idx as usize];
        (col.validity[row_idx as usize / 8] & (1u8 << (row_idx as usize % 8))) == 0
    }

    /// Column storage for direct access (Arrow encoding).
    pub fn get_column(&self, col_idx: u32) -> Option<&ColumnStorage> {
        self.columns.get(col_idx as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrowType, SignalSchemaField};

    #[test]
    fn parse_error_codes_match_ts() {
        // Values match the TypeScript EventLogError codes.
        assert_eq!(ParseError::Ok as u32, 0);
        assert_eq!(ParseError::InvalidJson as u32, 1);
        assert_eq!(ParseError::InvalidMsgpack as u32, 2);
        assert_eq!(ParseError::MissingField as u32, 3);
        assert_eq!(ParseError::InvalidFieldType as u32, 4);
        assert_eq!(ParseError::TooManyEvents as u32, 5);
        assert_eq!(ParseError::BufferOverflow as u32, 6);
        assert_eq!(ParseError::OutOfMemory as u32, 7);
    }

    fn field(arrow_type: ArrowType, nullable: bool) -> SignalSchemaField {
        SignalSchemaField::new(arrow_type, nullable)
    }

    // test "DynamicColumns - init and deinit"
    #[test]
    fn dynamic_init() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Utf8, false),
            field(ArrowType::Int64, false),
            field(ArrowType::Binary, true),
        ];
        let cols = DynamicColumns::new(&fields, 100);
        assert_eq!(cols.count, 0);
        assert_eq!(cols.field_count, 4);
    }

    // test "DynamicColumns - append values"
    #[test]
    fn dynamic_append_values() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Int32, false),
            field(ArrowType::Float64, true),
            field(ArrowType::Bool, true),
        ];
        let mut cols = DynamicColumns::new(&fields, 10);
        assert!(cols.begin_row());
        cols.append_variable(0, b"id-001").unwrap();
        cols.append_int(1, 42).unwrap();
        cols.append_float(2, 99.99).unwrap();
        cols.append_bool(3, true).unwrap();
        cols.end_row();
        assert_eq!(cols.count, 1);
        for col in 0..4 {
            assert!(!cols.is_null(col, 0));
        }
        assert_eq!(cols.columns[0].read_variable(0).unwrap(), b"id-001");
        assert_eq!(cols.columns[1].read_int(0).unwrap(), 42);
        assert_eq!(cols.columns[2].read_float(0).unwrap(), 99.99);
        assert!(cols.columns[3].read_bool(0).unwrap());
    }

    // test "DynamicColumns - null values"
    #[test]
    fn dynamic_null_values() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Float64, true),
        ];
        let mut cols = DynamicColumns::new(&fields, 10);
        assert!(cols.begin_row());
        cols.append_variable(0, b"id-001").unwrap();
        cols.append_null(1).unwrap();
        cols.end_row();
        assert!(!cols.is_null(0, 0));
        assert!(cols.is_null(1, 0));
    }

    // test "DynamicColumns - multiple rows"
    #[test]
    fn dynamic_multiple_rows() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Int32, false),
        ];
        let mut cols = DynamicColumns::new(&fields, 10);
        for (name, count) in [("alice", 10), ("bob", 20), ("charlie", 30)] {
            assert!(cols.begin_row());
            cols.append_variable(0, name.as_bytes()).unwrap();
            cols.append_int(1, count).unwrap();
            cols.end_row();
        }
        assert_eq!(cols.count, 3);
    }

    /// A four-byte plane is a signed Int32 end to end: what is appended is
    /// what is read back, and the unsigned upper half is refused rather than
    /// stored as a value no reader can name.
    #[test]
    fn int32_column_is_signed_end_to_end() {
        let fields = [field(ArrowType::Int32, false)];
        let mut cols = DynamicColumns::new(&fields, 8);
        for value in [0, -1, i64::from(i32::MIN), i64::from(i32::MAX)] {
            assert!(cols.begin_row());
            cols.append_int(0, value).unwrap();
            cols.end_row();
        }
        let plane = &cols.columns[0];
        assert_eq!(plane.read_int(0).unwrap(), 0);
        assert_eq!(plane.read_int(1).unwrap(), -1);
        assert_eq!(plane.read_int(2).unwrap(), i64::from(i32::MIN));
        assert_eq!(plane.read_int(3).unwrap(), i64::from(i32::MAX));

        assert!(cols.begin_row());
        assert_eq!(
            cols.append_int(0, i64::from(i32::MAX) + 1),
            Err(ParseError::InvalidFieldType)
        );
        assert_eq!(
            cols.append_int(0, i64::from(i32::MIN) - 1),
            Err(ParseError::InvalidFieldType)
        );
    }

    // test "DynamicColumns - reset for reuse"
    #[test]
    fn dynamic_reset() {
        let fields = [field(ArrowType::Utf8, false)];
        let mut cols = DynamicColumns::new(&fields, 10);
        assert!(cols.begin_row());
        cols.append_variable(0, b"test").unwrap();
        cols.end_row();
        assert_eq!(cols.count, 1);
        cols.reset();
        assert_eq!(cols.count, 0);
        assert!(cols.begin_row());
        cols.append_variable(0, b"new").unwrap();
        cols.end_row();
        assert_eq!(cols.count, 1);
        assert_eq!(cols.columns[0].read_variable(0).unwrap(), b"new");
    }

    // test "DynamicColumns - invalid column type error"
    #[test]
    fn dynamic_invalid_type() {
        let fields = [field(ArrowType::Int32, false)];
        let mut cols = DynamicColumns::new(&fields, 10);
        assert!(cols.begin_row());
        assert_eq!(
            cols.append_variable(0, b"not an int"),
            Err(ParseError::InvalidFieldType)
        );
    }

    // test "DynamicColumns retains two maximum measured Binary payloads"
    #[test]
    fn dynamic_retains_grown_binary_capacity() {
        let fields = [field(ArrowType::Binary, true)];
        let mut cols = DynamicColumns::new(&fields, 2);
        let payload = [0x5a_u8; 48_000];
        for _ in 0..2 {
            assert!(cols.begin_row());
            cols.append_variable(0, &payload).unwrap();
            cols.end_row();
        }
        assert_eq!(cols.columns[0].data_len, 96_000);
        let warm_capacity = cols.columns[0].data_capacity();
        for _ in 0..20 {
            cols.reset();
            for _ in 0..2 {
                assert!(cols.begin_row());
                cols.append_variable(0, &payload).unwrap();
                cols.end_row();
            }
            // Retained capacity avoids growth during warm reuse.
            assert_eq!(cols.columns[0].data_capacity(), warm_capacity);
            assert_eq!(cols.columns[0].data_len, 96_000);
        }
    }

    // test "DynamicColumns grows mixed variable columns and preserves prior rows"
    #[test]
    fn dynamic_grows_mixed_and_preserves() {
        let fields = [
            field(ArrowType::Utf8, false),
            field(ArrowType::Binary, true),
            field(ArrowType::Int64, false),
            field(ArrowType::Bool, true),
        ];
        let mut cols = DynamicColumns::new(&fields, 4);
        let first = [b'a'; 40_000];
        let second = [b'b'; 40_000];

        assert!(cols.begin_row());
        cols.append_variable(0, &first).unwrap();
        cols.append_variable(1, &second).unwrap();
        cols.append_int(2, 11).unwrap();
        cols.append_null(3).unwrap();
        cols.end_row();

        assert!(cols.begin_row());
        cols.append_variable(0, &second).unwrap();
        cols.append_variable(1, &first).unwrap();
        cols.append_int(2, 22).unwrap();
        cols.append_bool(3, true).unwrap();
        cols.end_row();

        let text = &cols.columns[0];
        let binary = &cols.columns[1];
        assert_eq!(text.data_len, 80_000);
        assert_eq!(binary.data_len, 80_000);
        assert_eq!(text.read_variable(0).unwrap(), &first);
        assert_eq!(text.read_variable(1).unwrap(), &second);
        assert_eq!(binary.read_variable(0).unwrap(), &second);
        assert_eq!(binary.read_variable(1).unwrap(), &first);
        assert_eq!(read_offset(&text.offsets, 1, 4), 40_000);
        assert_eq!(read_offset(&text.offsets, 2, 4), 80_000);
        assert!(cols.is_null(3, 0));
        assert!(!cols.is_null(3, 1));
    }

    // test "ColumnStorage configured maximum and reset retain grown allocation"
    #[test]
    fn column_storage_configured_maximum() {
        let mut col = ColumnStorage::with_variable_limit(PlaneKind::Bytes { offset_width: 4 }, 2, 1024);
        col.ensure_variable_capacity(1024).unwrap();
        assert_eq!(col.data_capacity(), 1024);
        for _ in 0..20 {
            col.reset();
            col.ensure_variable_capacity(1024).unwrap();
            assert_eq!(col.data_capacity(), 1024);
            col.data_len = 1024;
        }
        col.reset();
        assert_eq!(
            col.ensure_variable_capacity(1025),
            Err(ParseError::BufferOverflow)
        );
        assert_eq!(col.data_capacity(), 1024);
        assert_eq!(col.data_len, 0);
    }

    /// Duplicate JSON keys hit the same declared column twice in one row.
    /// The final offset points at the last append, while bytes from earlier
    /// appends remain in the backing buffer and are excluded from the cell.
    #[test]
    fn duplicate_append_leaves_dead_bytes() {
        let fields = [field(ArrowType::Utf8, false)];
        let mut cols = DynamicColumns::new(&fields, 4);
        assert!(cols.begin_row());
        cols.append_variable(0, b"first").unwrap();
        cols.append_variable(0, b"second").unwrap();
        cols.end_row();
        // The cell reads as the LAST append...
        assert_eq!(cols.columns[0].read_variable(0).unwrap(), b"second");
        // ...but data_len includes the dead first append (5 + 6 bytes).
        assert_eq!(cols.columns[0].data_len, 11);
        assert_eq!(read_offset(&cols.columns[0].offsets, 0, 4), 5);
    }

    /// A null append is a no-op, so appending binary data afterward publishes
    /// only the binary value and its validity bit.
    #[test]
    fn fallback_null_then_binary_is_clean() {
        let fields = [field(ArrowType::Binary, true)];
        let mut cols = DynamicColumns::new(&fields, 4);
        assert!(cols.begin_row());
        cols.append_null(0).unwrap();
        cols.append_variable(0, b"\xdf\0\0\0\0").unwrap();
        cols.end_row();
        assert!(!cols.is_null(0, 0));
        assert_eq!(cols.columns[0].data_len, 5);
        assert_eq!(read_offset(&cols.columns[0].offsets, 0, 4), 0);
    }

    /// Transactional reservation: nothing publishes until commit.
    #[test]
    fn reservation_commit_publishes() {
        let fields = [field(ArrowType::Binary, true)];
        let mut cols = DynamicColumns::new(&fields, 4);
        assert!(cols.begin_row());
        let reservation = cols.reserve_binary_value(0).unwrap();
        reservation.ensure_capacity(&mut cols, 3).unwrap();
        reservation.buffer(&mut cols)[..3].copy_from_slice(b"abc");
        // Not yet committed: cell is null, data_len untouched.
        assert_eq!(cols.columns[0].data_len, 0);
        reservation.commit(&mut cols, 3).unwrap();
        cols.end_row();
        assert!(!cols.is_null(0, 0));
        assert_eq!(cols.columns[0].read_variable(0).unwrap(), b"abc");
    }

    /// Reservation on a non-binary column refuses.
    #[test]
    fn reservation_requires_binary() {
        let fields = [field(ArrowType::Utf8, false)];
        let mut cols = DynamicColumns::new(&fields, 4);
        assert_eq!(
            cols.reserve_binary_value(0).unwrap_err(),
            VariableValueError::InvalidFieldType
        );
    }
}
