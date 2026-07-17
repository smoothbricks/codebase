//! Columnar buffers for parsed events (`parsing/columns.zig`, unified).
//!
//! Drift audit: the axe-runtime copy is the columine file minus the whole
//! `EventColumns` base path (296 deleted lines; its 5 added lines are
//! comments). The unified port keeps both storage types; the event-processor
//! crate parameterizes which path is wired.
//!
//! Arrow compatibility (same as Zig):
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

/// Error codes for parsing operations. The `u32` discriminants match the
/// TypeScript `EventLogError` codes (JS interop contract; columns.zig:33-42)
/// and are pinned by `parse_error_codes_match_ts`.
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

fn read_u32(bytes: &[u8], index: usize) -> u32 {
    let start = index * 4;
    u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap_or([0; 4]))
}

fn write_u32(bytes: &mut [u8], index: usize, value: u32) {
    let start = index * 4;
    bytes[start..start + 4].copy_from_slice(&value.to_le_bytes());
}

/// Columnar buffers for the base 4-column event schema
/// (columns.zig `EventColumns`; columine-only — the axe fork deleted it).
///
/// Buffer capacities are the Zig estimates and hard limits: exceeding them
/// is `BUFFER_OVERFLOW`, exactly like the fixed Zig allocations.
#[derive(Clone, Debug)]
pub struct EventColumns {
    /// Number of events.
    pub count: u32,

    // ID column (string - UUID format, typically 36 chars)
    id_offsets: Vec<u8>,
    id_data: Vec<u8>,
    pub id_data_len: u32,

    // Type column (string - event type identifier)
    type_offsets: Vec<u8>,
    type_data: Vec<u8>,
    pub type_data_len: u32,

    // Timestamp column (i64 microseconds since Unix epoch), LE bytes.
    timestamps: Vec<u8>,
    capacity: u32,

    // Value column (binary - serialized JSON/msgpack, nullable)
    value_offsets: Vec<u8>,
    value_data: Vec<u8>,
    pub value_data_len: u32,
    /// Bit-packed: bit i = 1 if event i has value (Arrow LSB-first).
    value_nulls: Vec<u8>,
}

impl EventColumns {
    /// Initialize with the given capacity, clamped to
    /// [`MAX_EVENTS_PER_BATCH`]. Buffer sizes are the Zig per-event
    /// estimates (id 36 B, type 64 B, value 256 B).
    pub fn new(capacity: u32) -> Self {
        let cap = capacity.min(MAX_EVENTS_PER_BATCH);
        let offsets_len = (cap as usize + 1) * 4;
        Self {
            count: 0,
            id_offsets: vec![0; offsets_len],
            id_data: vec![0; cap as usize * 36], // UUID = 36 chars
            id_data_len: 0,
            type_offsets: vec![0; offsets_len],
            type_data: vec![0; cap as usize * 64], // avg type name estimate
            type_data_len: 0,
            timestamps: vec![0; cap as usize * 8],
            capacity: cap,
            value_offsets: vec![0; offsets_len],
            value_data: vec![0; cap as usize * 256], // avg value size estimate
            value_data_len: 0,
            value_nulls: vec![0; (cap as usize).div_ceil(8)],
        }
    }

    /// Reset for reuse without reallocating.
    pub fn reset(&mut self) {
        self.count = 0;
        self.id_data_len = 0;
        self.type_data_len = 0;
        self.value_data_len = 0;
        self.value_nulls.fill(0);
    }

    /// Add an event (columns.zig `addEvent`): strings are copied into the
    /// internal data buffers; hitting a fixed buffer limit is
    /// `BufferOverflow`, a full batch is `TooManyEvents`.
    pub fn add_event(
        &mut self,
        id: &[u8],
        event_type: &[u8],
        timestamp_us: i64,
        value: Option<&[u8]>,
    ) -> Result<(), ParseError> {
        if self.count >= self.capacity {
            return Err(ParseError::TooManyEvents);
        }
        let idx = self.count as usize;

        // ID column
        write_u32(&mut self.id_offsets, idx, self.id_data_len);
        if self.id_data_len as usize + id.len() > self.id_data.len() {
            return Err(ParseError::BufferOverflow);
        }
        self.id_data[self.id_data_len as usize..][..id.len()].copy_from_slice(id);
        self.id_data_len += id.len() as u32;

        // Type column
        write_u32(&mut self.type_offsets, idx, self.type_data_len);
        if self.type_data_len as usize + event_type.len() > self.type_data.len() {
            return Err(ParseError::BufferOverflow);
        }
        self.type_data[self.type_data_len as usize..][..event_type.len()]
            .copy_from_slice(event_type);
        self.type_data_len += event_type.len() as u32;

        // Timestamp column
        self.timestamps[idx * 8..idx * 8 + 8].copy_from_slice(&timestamp_us.to_le_bytes());

        // Value column (nullable)
        write_u32(&mut self.value_offsets, idx, self.value_data_len);
        if let Some(v) = value {
            if self.value_data_len as usize + v.len() > self.value_data.len() {
                return Err(ParseError::BufferOverflow);
            }
            self.value_data[self.value_data_len as usize..][..v.len()].copy_from_slice(v);
            self.value_data_len += v.len() as u32;
            // Set validity bit (1 = valid, Arrow LSB-first)
            self.value_nulls[idx / 8] |= 1u8 << (idx % 8);
        }
        // else: leave null bit as 0 (null)

        self.count += 1;

        // Set final offset (Arrow requires n+1 offsets)
        let count = self.count as usize;
        write_u32(&mut self.id_offsets, count, self.id_data_len);
        write_u32(&mut self.type_offsets, count, self.type_data_len);
        write_u32(&mut self.value_offsets, count, self.value_data_len);

        Ok(())
    }

    pub fn get_id(&self, idx: u32) -> Option<&[u8]> {
        if idx >= self.count {
            return None;
        }
        let start = read_u32(&self.id_offsets, idx as usize) as usize;
        let end = read_u32(&self.id_offsets, idx as usize + 1) as usize;
        Some(&self.id_data[start..end])
    }

    pub fn get_type(&self, idx: u32) -> Option<&[u8]> {
        if idx >= self.count {
            return None;
        }
        let start = read_u32(&self.type_offsets, idx as usize) as usize;
        let end = read_u32(&self.type_offsets, idx as usize + 1) as usize;
        Some(&self.type_data[start..end])
    }

    pub fn get_timestamp(&self, idx: u32) -> Option<i64> {
        if idx >= self.count {
            return None;
        }
        let start = idx as usize * 8;
        Some(i64::from_le_bytes(
            self.timestamps[start..start + 8].try_into().ok()?,
        ))
    }

    /// True if the event at `idx` has a value (not null).
    pub fn has_value(&self, idx: u32) -> bool {
        if idx >= self.count {
            return false;
        }
        let idx = idx as usize;
        (self.value_nulls[idx / 8] & (1u8 << (idx % 8))) != 0
    }

    pub fn get_value(&self, idx: u32) -> Option<&[u8]> {
        if idx >= self.count || !self.has_value(idx) {
            return None;
        }
        let start = read_u32(&self.value_offsets, idx as usize) as usize;
        let end = read_u32(&self.value_offsets, idx as usize + 1) as usize;
        Some(&self.value_data[start..end])
    }

    // IPC-writer views (columns.zig exposes the raw slices; here they are
    // explicit borrow methods over the byte-backed storage).

    pub fn id_offsets_bytes(&self) -> &[u8] {
        &self.id_offsets[..(self.count as usize + 1) * 4]
    }
    pub fn id_data_bytes(&self) -> &[u8] {
        &self.id_data[..self.id_data_len as usize]
    }
    pub fn type_offsets_bytes(&self) -> &[u8] {
        &self.type_offsets[..(self.count as usize + 1) * 4]
    }
    pub fn type_data_bytes(&self) -> &[u8] {
        &self.type_data[..self.type_data_len as usize]
    }
    pub fn timestamps_bytes(&self) -> &[u8] {
        &self.timestamps[..self.count as usize * 8]
    }
    pub fn value_offsets_bytes(&self) -> &[u8] {
        &self.value_offsets[..(self.count as usize + 1) * 4]
    }
    pub fn value_data_bytes(&self) -> &[u8] {
        &self.value_data[..self.value_data_len as usize]
    }
    pub fn value_nulls_bytes(&self) -> &[u8] {
        &self.value_nulls[..(self.count as usize).div_ceil(8)]
    }
}

/// Column type determines storage layout (columns.zig `ColumnType`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    /// Variable-length string: offsets + data.
    Utf8,
    /// Fixed 8 bytes per value.
    Int64,
    /// Fixed 8 bytes per value.
    Float64,
    /// Bit-packed (1 bit per value).
    Bool,
    /// Variable-length binary: offsets + data.
    Binary,
}

/// Storage for a single column of typed data (columns.zig `ColumnStorage`).
/// All columns are nullable (validity bitmap always present).
#[derive(Clone, Debug)]
pub struct ColumnStorage {
    pub col_type: ColumnType,

    /// Validity bitmap (Arrow LSB-first).
    validity: Vec<u8>,

    // For utf8/binary: offsets (u32 LE bytes) and data.
    offsets: Option<Vec<u8>>,
    data: Option<Vec<u8>>,
    pub data_len: u32,
    /// Configured hard limit for retained variable-width data.
    max_data_bytes: u32,

    // For int32/int64/float64: fixed-width little-endian values.
    fixed: Option<Vec<u8>>,
    fixed_width: u8,

    // For bool: bit-packed data.
    bool_data: Option<Vec<u8>>,
}

impl ColumnStorage {
    pub fn new(col_type: ColumnType, capacity: u32) -> Self {
        Self::with_variable_limit(col_type, capacity, MAX_VALUE_BYTES)
    }

    pub fn with_variable_limit(col_type: ColumnType, capacity: u32, max_data_bytes: u32) -> Self {
        Self::with_fixed_width(col_type, capacity, max_data_bytes, 8)
    }

    fn with_fixed_width(
        col_type: ColumnType,
        capacity: u32,
        max_data_bytes: u32,
        fixed_width: u8,
    ) -> Self {
        let cap = capacity.min(MAX_EVENTS_PER_BATCH) as usize;
        let mut storage = Self {
            col_type,
            validity: vec![0; cap.div_ceil(8)],
            offsets: None,
            data: None,
            data_len: 0,
            max_data_bytes,
            fixed: None,
            fixed_width: 0,
            bool_data: None,
        };
        match col_type {
            ColumnType::Utf8 | ColumnType::Binary => {
                storage.offsets = Some(vec![0; (cap + 1) * 4]);
                let initial_bytes = (max_data_bytes as usize).min(cap * 128);
                storage.data = Some(vec![0; initial_bytes]);
            }
            ColumnType::Int64 | ColumnType::Float64 => {
                storage.fixed_width = fixed_width;
                storage.fixed = Some(vec![0; cap * fixed_width as usize]);
            }
            ColumnType::Bool => {
                storage.bool_data = Some(vec![0; cap.div_ceil(8)]);
            }
        }
        storage
    }

    /// Int32 rides the Int64 column kind with 4-byte storage; allocate the
    /// fixed buffer once at the right width instead of building the 8-byte
    /// buffer via `new()` and immediately replacing it.
    fn new_int32(capacity: u32) -> Self {
        Self::with_fixed_width(ColumnType::Int64, capacity, MAX_VALUE_BYTES, 4)
    }

    /// Reset for reuse. Grown variable-width allocations are retained
    /// (pinned by the Zig warm-pointer test; here pinned as retained
    /// capacity).
    pub fn reset(&mut self) {
        self.validity.fill(0);
        self.data_len = 0;
        if let Some(b) = self.bool_data.as_mut() {
            b.fill(0);
        }
    }

    /// Grow the variable-width data buffer geometrically, capped at
    /// `max_data_bytes` (columns.zig `ensureVariableCapacityPreserving`).
    /// Only `preserve_end` bytes survive the reallocation, exactly like the
    /// Zig alloc+memcpy+free (bytes past it are NOT carried over).
    fn ensure_variable_capacity_preserving(
        &mut self,
        required: usize,
        preserve_end: usize,
    ) -> Result<(), ParseError> {
        let Some(current) = self.data.as_mut() else {
            return Err(ParseError::InvalidFieldType);
        };
        if required <= current.len() {
            return Ok(());
        }
        if required > self.max_data_bytes as usize {
            return Err(ParseError::BufferOverflow);
        }

        let max = self.max_data_bytes as usize;
        let mut new_capacity = current.len();
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
        debug_assert!(preserve_end <= current.len());
        replacement[..preserve_end].copy_from_slice(&current[..preserve_end]);
        *current = replacement;
        Ok(())
    }

    fn ensure_variable_capacity(&mut self, required: usize) -> Result<(), ParseError> {
        self.ensure_variable_capacity_preserving(required, self.data_len as usize)
    }

    pub fn validity_bytes(&self, row_count: u32) -> &[u8] {
        &self.validity[..(row_count as usize).div_ceil(8)]
    }
    pub fn offsets_bytes(&self, row_count: u32) -> Option<&[u8]> {
        Some(&self.offsets.as_ref()?[..(row_count as usize + 1) * 4])
    }
    pub fn data_bytes(&self) -> Option<&[u8]> {
        Some(&self.data.as_ref()?[..self.data_len as usize])
    }
    /// Full retained data capacity (Zig exposes the slice; tests pin
    /// warm-reuse via this).
    pub fn data_capacity(&self) -> Option<usize> {
        Some(self.data.as_ref()?.len())
    }
    pub fn fixed_i32_bytes(&self, row_count: u32) -> Option<&[u8]> {
        Some(&self.fixed.as_ref()?[..row_count as usize * 4])
    }
    pub fn fixed_i64_bytes(&self, row_count: u32) -> Option<&[u8]> {
        Some(&self.fixed.as_ref()?[..row_count as usize * 8])
    }
    pub fn fixed_f64_bytes(&self, row_count: u32) -> Option<&[u8]> {
        Some(&self.fixed.as_ref()?[..row_count as usize * 8])
    }
    pub fn bool_bytes(&self, row_count: u32) -> Option<&[u8]> {
        Some(&self.bool_data.as_ref()?[..(row_count as usize).div_ceil(8)])
    }

    pub fn read_fixed_i32(&self, row: u32) -> Option<u32> {
        let fixed = self.fixed.as_ref()?;
        let start = row as usize * 4;
        Some(u32::from_le_bytes(fixed[start..start + 4].try_into().ok()?))
    }
    pub fn read_fixed_i64(&self, row: u32) -> Option<i64> {
        let fixed = self.fixed.as_ref()?;
        if self.fixed_width == 4 {
            return self.read_fixed_i32(row).map(i64::from);
        }
        let start = row as usize * 8;
        Some(i64::from_le_bytes(fixed[start..start + 8].try_into().ok()?))
    }
    pub fn read_fixed_f64(&self, row: u32) -> Option<f64> {
        let fixed = self.fixed.as_ref()?;
        let start = row as usize * 8;
        Some(f64::from_le_bytes(fixed[start..start + 8].try_into().ok()?))
    }
    pub fn read_bool(&self, row: u32) -> Option<bool> {
        let bits = self.bool_data.as_ref()?;
        Some((bits[row as usize / 8] & (1u8 << (row as usize % 8))) != 0)
    }
    pub fn read_variable(&self, row: u32) -> Option<&[u8]> {
        let offsets = self.offsets.as_ref()?;
        let data = self.data.as_ref()?;
        let start = read_u32(offsets, row as usize) as usize;
        let end = read_u32(offsets, row as usize + 1) as usize;
        Some(&data[start..end])
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

/// Transactional writer for one variable-width cell (columns.zig
/// `VariableValueReservation`): capacity growth is retained, but offset,
/// length, and validity are published only by `commit`, so a parser failure
/// leaves the column logically unchanged.
///
/// The Zig version holds a raw column pointer; the Rust port names the
/// column by index and borrows the parent per call (no aliasing).
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

    /// The writable tail of the column's data buffer, starting at the
    /// reservation.
    pub fn buffer<'a>(&self, cols: &'a mut DynamicColumns) -> &'a mut [u8] {
        let column = &mut cols.columns[self.col_idx as usize];
        let data = column
            .data
            .as_mut()
            .unwrap_or_else(|| columine_types::die!("reservation on non-variable column"));
        &mut data[self.start as usize..]
    }

    /// Publish the cell: offset, data length, and validity (columns.zig
    /// `commit`).
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
        let row = self.row_index as usize;
        write_u32(
            column
                .offsets
                .as_mut()
                .unwrap_or_else(|| columine_types::die!("reservation on non-variable column")),
            row,
            self.start,
        );
        column.data_len = end as u32;
        column.validity[row / 8] |= 1u8 << (row % 8);
        Ok(())
    }
}

/// Dynamic columnar buffers for N-column extraction (columns.zig
/// `DynamicColumns`). All value.* columns are nullable because events may
/// not contain all declared fields (sparse data).
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

use crate::schema::{ArrowType, SignalSchemaField};

impl DynamicColumns {
    /// Initialize from schema field metadata (columns.zig `init`): each
    /// Arrow type maps to its storage layout; `Null` is stored as utf8
    /// ("treat null type as empty strings").
    pub fn new(field_metadata: &[SignalSchemaField], capacity: u32) -> Self {
        let cap = capacity.min(MAX_EVENTS_PER_BATCH);
        let columns = field_metadata
            .iter()
            .map(|field| match field.arrow_type {
                ArrowType::Int32 => ColumnStorage::new_int32(cap),
                ArrowType::Utf8 => ColumnStorage::new(ColumnType::Utf8, cap),
                ArrowType::Binary => ColumnStorage::new(ColumnType::Binary, cap),
                ArrowType::Int64 => ColumnStorage::new(ColumnType::Int64, cap),
                ArrowType::Float64 => ColumnStorage::new(ColumnType::Float64, cap),
                ArrowType::Bool => ColumnStorage::new(ColumnType::Bool, cap),
                ArrowType::Null => ColumnStorage::new(ColumnType::Utf8, cap),
            })
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

    /// Begin a new row; false when at capacity (columns.zig `beginRow` —
    /// note it does NOT allocate row state; appends target `self.count`).
    pub fn begin_row(&mut self) -> bool {
        self.count < self.capacity
    }

    /// Complete the current row: bump the count and publish the n+1 offset
    /// for every variable-length column (columns.zig `endRow`).
    pub fn end_row(&mut self) {
        self.count += 1;
        let count = self.count as usize;
        for col in &mut self.columns {
            let data_len = col.data_len;
            if let Some(offsets) = col.offsets.as_mut() {
                write_u32(offsets, count, data_len);
            }
        }
    }

    /// WHY (fix the deleted Zig lacked): Zig had no abandonRow — its extractor error paths simply
    /// never call endRow, leaving appended bytes/validity for the dead row
    /// unpublished (count is not bumped). Dropping the pending row here is
    /// the same observable behavior; kept as an explicit method because the
    /// Rust extractors call it on their error paths.
    pub fn abandon_row(&mut self) {
        // Appends target row `count`; without end_row the row never becomes
        // visible. Validity bits set for the dead row are masked by count on
        // every read path, exactly as in Zig.
    }

    /// Reserve a transactional writer for a binary cell (columns.zig
    /// `reserveBinaryValue`).
    pub fn reserve_binary_value(
        &mut self,
        col_idx: u32,
    ) -> Result<VariableValueReservation, VariableValueError> {
        if col_idx >= self.field_count {
            return Err(VariableValueError::InvalidFieldType);
        }
        let column = &self.columns[col_idx as usize];
        if column.col_type != ColumnType::Binary {
            return Err(VariableValueError::InvalidFieldType);
        }
        Ok(VariableValueReservation {
            col_idx,
            row_index: self.count,
            start: column.data_len,
        })
    }

    /// Append a UTF-8 string value (columns.zig `appendUtf8`).
    ///
    /// Faithful append semantics: the row's offset is set to the CURRENT
    /// data_len and bytes are appended. A second append to the same cell in
    /// one row moves the offset forward and leaves the first bytes as dead
    /// data between offsets — observable in the IPC body byte image (pinned
    /// by `duplicate_append_leaves_dead_bytes`).
    pub fn append_utf8(&mut self, col_idx: u32, value: &[u8]) -> Result<(), ParseError> {
        if col_idx >= self.field_count {
            return Err(ParseError::InvalidFieldType);
        }
        let row_idx = self.count as usize;
        let col = &mut self.columns[col_idx as usize];
        if col.col_type != ColumnType::Utf8 && col.col_type != ColumnType::Binary {
            return Err(ParseError::InvalidFieldType);
        }
        let required = (col.data_len as usize)
            .checked_add(value.len())
            .ok_or(ParseError::BufferOverflow)?;
        col.ensure_variable_capacity(required)?;

        let data_len = col.data_len;
        write_u32(
            col.offsets
                .as_mut()
                .unwrap_or_else(|| columine_types::die!("variable column without offsets")),
            row_idx,
            data_len,
        );
        let data = col
            .data
            .as_mut()
            .unwrap_or_else(|| columine_types::die!("variable column without data"));
        data[data_len as usize..][..value.len()].copy_from_slice(value);
        col.data_len += value.len() as u32;
        col.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
        Ok(())
    }

    /// Append a physical 32-bit value. Negative signed values and the full
    /// UInt32 domain share the same four-byte Arrow representation.
    pub fn append_int32(&mut self, col_idx: u32, value: i64) -> Result<(), ParseError> {
        if self
            .columns
            .get(col_idx as usize)
            .is_none_or(|column| column.fixed_width != 4)
        {
            return Err(ParseError::InvalidFieldType);
        }
        self.append_int64(col_idx, value)
    }

    /// Append an Int64 value (columns.zig `appendInt64`).
    pub fn append_int64(&mut self, col_idx: u32, value: i64) -> Result<(), ParseError> {
        if col_idx >= self.field_count {
            return Err(ParseError::InvalidFieldType);
        }
        let row_idx = self.count as usize;
        let col = &mut self.columns[col_idx as usize];
        let fixed = col.fixed.as_mut().ok_or(ParseError::InvalidFieldType)?;
        match (col.col_type, col.fixed_width) {
            (ColumnType::Int64, 4) => {
                if !(i64::from(i32::MIN)..=i64::from(u32::MAX)).contains(&value) {
                    return Err(ParseError::InvalidFieldType);
                }
                fixed[row_idx * 4..row_idx * 4 + 4].copy_from_slice(&(value as u32).to_le_bytes());
            }
            (ColumnType::Int64, 8) => {
                fixed[row_idx * 8..row_idx * 8 + 8].copy_from_slice(&value.to_le_bytes());
            }
            _ => return Err(ParseError::InvalidFieldType),
        }
        col.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
        Ok(())
    }

    /// Append a Float64 value (columns.zig `appendFloat64`).
    pub fn append_float64(&mut self, col_idx: u32, value: f64) -> Result<(), ParseError> {
        if col_idx >= self.field_count {
            return Err(ParseError::InvalidFieldType);
        }
        let row_idx = self.count as usize;
        let col = &mut self.columns[col_idx as usize];
        if col.col_type != ColumnType::Float64 {
            return Err(ParseError::InvalidFieldType);
        }
        let fixed = col
            .fixed
            .as_mut()
            .unwrap_or_else(|| columine_types::die!("float64 column without fixed storage"));
        fixed[row_idx * 8..row_idx * 8 + 8].copy_from_slice(&value.to_le_bytes());
        col.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
        Ok(())
    }

    /// Append a boolean value (columns.zig `appendBool`, Arrow LSB-first).
    pub fn append_bool(&mut self, col_idx: u32, value: bool) -> Result<(), ParseError> {
        if col_idx >= self.field_count {
            return Err(ParseError::InvalidFieldType);
        }
        let row_idx = self.count as usize;
        let col = &mut self.columns[col_idx as usize];
        if col.col_type != ColumnType::Bool {
            return Err(ParseError::InvalidFieldType);
        }
        if value {
            let bits = col
                .bool_data
                .as_mut()
                .unwrap_or_else(|| columine_types::die!("bool column without bit storage"));
            bits[row_idx / 8] |= 1u8 << (row_idx % 8);
        }
        col.validity[row_idx / 8] |= 1u8 << (row_idx % 8);
        Ok(())
    }

    /// Append binary data (columns.zig `appendBinary` — same storage as
    /// UTF-8).
    pub fn append_binary(&mut self, col_idx: u32, value: &[u8]) -> Result<(), ParseError> {
        self.append_utf8(col_idx, value)
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
        // columns.zig:33-42 — values match TypeScript EventLogError codes.
        assert_eq!(ParseError::Ok as u32, 0);
        assert_eq!(ParseError::InvalidJson as u32, 1);
        assert_eq!(ParseError::InvalidMsgpack as u32, 2);
        assert_eq!(ParseError::MissingField as u32, 3);
        assert_eq!(ParseError::InvalidFieldType as u32, 4);
        assert_eq!(ParseError::TooManyEvents as u32, 5);
        assert_eq!(ParseError::BufferOverflow as u32, 6);
        assert_eq!(ParseError::OutOfMemory as u32, 7);
    }

    // test "EventColumns - init and deinit"
    #[test]
    fn event_columns_init() {
        let cols = EventColumns::new(100);
        assert_eq!(cols.count, 0);
    }

    // test "EventColumns - add single event"
    #[test]
    fn event_columns_add_single() {
        let mut cols = EventColumns::new(10);
        cols.add_event(
            b"550e8400-e29b-41d4-a716-446655440000",
            b"orderPlaced",
            1_705_315_800_000_000,
            Some(br#"{"orderId":"123"}"#),
        )
        .unwrap();
        assert_eq!(cols.count, 1);
        assert_eq!(
            cols.get_id(0).unwrap(),
            b"550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(cols.get_type(0).unwrap(), b"orderPlaced");
        assert_eq!(cols.get_timestamp(0).unwrap(), 1_705_315_800_000_000);
        assert!(cols.has_value(0));
        assert_eq!(cols.get_value(0).unwrap(), br#"{"orderId":"123"}"#);
    }

    // test "EventColumns - null value"
    #[test]
    fn event_columns_null_value() {
        let mut cols = EventColumns::new(10);
        cols.add_event(b"test-id", b"testEvent", 0, None).unwrap();
        assert!(!cols.has_value(0));
        assert_eq!(cols.get_value(0), None);
    }

    // test "EventColumns - multiple events"
    #[test]
    fn event_columns_multiple() {
        let mut cols = EventColumns::new(10);
        cols.add_event(b"id-1", b"type-a", 100, Some(b"value1"))
            .unwrap();
        cols.add_event(b"id-2", b"type-b", 200, None).unwrap();
        cols.add_event(b"id-3", b"type-a", 300, Some(b"value3"))
            .unwrap();
        assert_eq!(cols.count, 3);
        assert_eq!(cols.get_id(1).unwrap(), b"id-2");
        assert_eq!(cols.get_type(1).unwrap(), b"type-b");
        assert_eq!(cols.get_timestamp(1).unwrap(), 200);
        assert!(!cols.has_value(1));
        assert!(cols.has_value(0));
        assert!(cols.has_value(2));
    }

    // test "EventColumns - reset for reuse"
    #[test]
    fn event_columns_reset() {
        let mut cols = EventColumns::new(10);
        cols.add_event(b"id-1", b"type-a", 100, Some(b"value1"))
            .unwrap();
        assert_eq!(cols.count, 1);
        cols.reset();
        assert_eq!(cols.count, 0);
        cols.add_event(b"id-2", b"type-b", 200, None).unwrap();
        assert_eq!(cols.count, 1);
        assert_eq!(cols.get_id(0).unwrap(), b"id-2");
    }

    // test "EventColumns - too many events"
    #[test]
    fn event_columns_too_many() {
        let mut cols = EventColumns::new(2);
        cols.add_event(b"id-1", b"t", 0, None).unwrap();
        cols.add_event(b"id-2", b"t", 0, None).unwrap();
        assert_eq!(
            cols.add_event(b"id-3", b"t", 0, None),
            Err(ParseError::TooManyEvents)
        );
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
        cols.append_utf8(0, b"id-001").unwrap();
        cols.append_int64(1, 42).unwrap();
        cols.append_float64(2, 99.99).unwrap();
        cols.append_bool(3, true).unwrap();
        cols.end_row();
        assert_eq!(cols.count, 1);
        for col in 0..4 {
            assert!(!cols.is_null(col, 0));
        }
        assert_eq!(cols.columns[0].read_variable(0).unwrap(), b"id-001");
        assert_eq!(cols.columns[1].read_fixed_i64(0).unwrap(), 42);
        assert_eq!(cols.columns[2].read_fixed_f64(0).unwrap(), 99.99);
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
        cols.append_utf8(0, b"id-001").unwrap();
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
            cols.append_utf8(0, name.as_bytes()).unwrap();
            cols.append_int64(1, count).unwrap();
            cols.end_row();
        }
        assert_eq!(cols.count, 3);
    }

    // test "DynamicColumns - reset for reuse"
    #[test]
    fn dynamic_reset() {
        let fields = [field(ArrowType::Utf8, false)];
        let mut cols = DynamicColumns::new(&fields, 10);
        assert!(cols.begin_row());
        cols.append_utf8(0, b"test").unwrap();
        cols.end_row();
        assert_eq!(cols.count, 1);
        cols.reset();
        assert_eq!(cols.count, 0);
        assert!(cols.begin_row());
        cols.append_utf8(0, b"new").unwrap();
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
            cols.append_utf8(0, b"not an int"),
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
            cols.append_binary(0, &payload).unwrap();
            cols.end_row();
        }
        assert_eq!(cols.columns[0].data_len, 96_000);
        let warm_capacity = cols.columns[0].data_capacity().unwrap();
        for _ in 0..20 {
            cols.reset();
            for _ in 0..2 {
                assert!(cols.begin_row());
                cols.append_binary(0, &payload).unwrap();
                cols.end_row();
            }
            // Zig pins the warm pointer; Vec addresses aren't stable to
            // observe, so the retained-capacity contract is pinned instead:
            // no growth happens on warm reuse.
            assert_eq!(cols.columns[0].data_capacity().unwrap(), warm_capacity);
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
        cols.append_utf8(0, &first).unwrap();
        cols.append_binary(1, &second).unwrap();
        cols.append_int64(2, 11).unwrap();
        cols.append_null(3).unwrap();
        cols.end_row();

        assert!(cols.begin_row());
        cols.append_utf8(0, &second).unwrap();
        cols.append_binary(1, &first).unwrap();
        cols.append_int64(2, 22).unwrap();
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
        assert_eq!(read_u32(text.offsets.as_ref().unwrap(), 1), 40_000);
        assert_eq!(read_u32(text.offsets.as_ref().unwrap(), 2), 80_000);
        assert!(cols.is_null(3, 0));
        assert!(!cols.is_null(3, 1));
    }

    // test "ColumnStorage configured maximum and reset retain grown allocation"
    #[test]
    fn column_storage_configured_maximum() {
        let mut col = ColumnStorage::with_variable_limit(ColumnType::Binary, 2, 1024);
        col.ensure_variable_capacity(1024).unwrap();
        assert_eq!(col.data_capacity().unwrap(), 1024);
        for _ in 0..20 {
            col.reset();
            col.ensure_variable_capacity(1024).unwrap();
            assert_eq!(col.data_capacity().unwrap(), 1024);
            col.data_len = 1024;
        }
        col.reset();
        assert_eq!(
            col.ensure_variable_capacity(1025),
            Err(ParseError::BufferOverflow)
        );
        assert_eq!(col.data_capacity().unwrap(), 1024);
        assert_eq!(col.data_len, 0);
    }

    // Zig's OOM-preservation test simulates allocator failure; Rust's global
    // allocator aborts instead, so the preserved-slice contract is pinned on
    // the overflow path above (published slice untouched after refusal).

    /// Duplicate JSON keys hit the same declared column twice in one row:
    /// Zig append semantics move the offset and leave the first bytes dead
    /// in the data buffer — observable in the IPC body image. NOT a bug to
    /// fix silently; byte parity depends on it until cutover.
    #[test]
    fn duplicate_append_leaves_dead_bytes() {
        let fields = [field(ArrowType::Utf8, false)];
        let mut cols = DynamicColumns::new(&fields, 4);
        assert!(cols.begin_row());
        cols.append_utf8(0, b"first").unwrap();
        cols.append_utf8(0, b"second").unwrap();
        cols.end_row();
        // The cell reads as the LAST append...
        assert_eq!(cols.columns[0].read_variable(0).unwrap(), b"second");
        // ...but data_len includes the dead first append (5 + 6 bytes).
        assert_eq!(cols.columns[0].data_len, 11);
        assert_eq!(read_u32(cols.columns[0].offsets.as_ref().unwrap(), 0), 5);
    }

    /// The appendNull-then-appendBinary sequence the extractor performs on
    /// the fallback column (json_extractor.zig:257-271): appendNull is a
    /// no-op, so no dead bytes and validity comes only from appendBinary —
    /// the "double-append" flagged in the 4a review is in fact benign.
    #[test]
    fn fallback_null_then_binary_is_clean() {
        let fields = [field(ArrowType::Binary, true)];
        let mut cols = DynamicColumns::new(&fields, 4);
        assert!(cols.begin_row());
        cols.append_null(0).unwrap();
        cols.append_binary(0, b"\xdf\0\0\0\0").unwrap();
        cols.end_row();
        assert!(!cols.is_null(0, 0));
        assert_eq!(cols.columns[0].data_len, 5);
        assert_eq!(read_u32(cols.columns[0].offsets.as_ref().unwrap(), 0), 0);
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
