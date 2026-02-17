// =============================================================================
// Freelist Allocator for SpanBuffer Storage
// =============================================================================
//
// Implements spec 01q: WASM Memory Architecture for SpanBuffer Storage
//
// Memory Layout:
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ Header (64 bytes, cache-aligned)                                            │
// ├─────────────────────────────────────────────────────────────────────────────┤
// │ Allocated Blocks (spans + columns)                                          │
// ├─────────────────────────────────────────────────────────────────────────────┤
// │ Free Space (bump allocation area) ↑ bump_ptr                                │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// Size Classes:
// - Span System:  capacity × 9 + 4 bytes (timestamp + entry_type + writeIndex)
// - 1B Column:    ceil(capacity/8) + capacity × 1 (nulls + uint8/bool values)
// - 4B Column:    ceil(capacity/8) + capacity × 4 (nulls + uint32/float32 values)
// - 8B Column:    ceil(capacity/8) + capacity × 8 (nulls + float64/bigint64 values)
//
// Freelist: Zero-overhead, uses block's first 4 bytes as next pointer when free.
//

const std = @import("std");

// =============================================================================
// Configuration (set via init)
// =============================================================================

var g_capacity: u32 = 64; // Default, can be changed via init()

// Pre-computed block sizes (updated on init)
var g_span_system_size: u32 = 0;
var g_col_1b_size: u32 = 0;
var g_col_4b_size: u32 = 0;
var g_col_8b_size: u32 = 0;
var g_null_bitmap_size: u32 = 0;

// =============================================================================
// Header Layout (64 bytes, cache-aligned)
// =============================================================================

const HEADER_SIZE: u32 = 64;

// Header offsets
const OFFSET_BUMP_PTR: u32 = 0; // u32: Next free byte
const OFFSET_CAPACITY: u32 = 4; // u32: Rows per span
const OFFSET_FREELIST_SPAN: u32 = 8; // u32: Head of Span System freelist
const OFFSET_FREELIST_1B: u32 = 12; // u32: Head of 1-byte column freelist
const OFFSET_FREELIST_4B: u32 = 16; // u32: Head of 4-byte column freelist
const OFFSET_FREELIST_8B: u32 = 20; // u32: Head of 8-byte column freelist
const OFFSET_ALLOC_COUNT: u32 = 24; // u32: Total allocations (debug)
const OFFSET_FREE_COUNT: u32 = 28; // u32: Total frees (debug)
// 32-63: reserved

// =============================================================================
// SpanBuffer._system Layout
// =============================================================================
//
// [timestamp: i64 × capacity]       offset 0
// [entry_type: u8 × capacity]       offset capacity × 8
// [_writeIndex: u32]                offset capacity × 9
//
// Total: capacity × 9 + 4 bytes (aligned to 8 for BigInt64Array)

inline fn getSystemTimestampOffset() u32 {
    return 0;
}

inline fn getSystemEntryTypeOffset() u32 {
    return g_capacity * 8;
}

inline fn getSystemWriteIndexOffset() u32 {
    return g_capacity * 9;
}

// =============================================================================
// Entry Type Constants (per specs/lmao/01h and systemSchema.ts)
// =============================================================================

const ENTRY_TYPE_SPAN_START: u8 = 1;
const ENTRY_TYPE_SPAN_OK: u8 = 2;
const ENTRY_TYPE_SPAN_ERR: u8 = 3;
const ENTRY_TYPE_SPAN_EXCEPTION: u8 = 4; // NOT 5 - that's TRACE

// =============================================================================
// Header Read/Write Helpers
// =============================================================================

// WASM memory starts at address 0. Use allowzero pointers to access the full memory.

/// Get a pointer to WASM memory at the given offset.
/// Returns [*]allowzero u8 to handle offset 0 (header location).
inline fn ptrAt(offset: u32) [*]allowzero u8 {
    return @ptrFromInt(offset);
}

inline fn readU32(offset: u32) u32 {
    const ptr: *align(1) allowzero const u32 = @ptrCast(ptrAt(offset));
    return ptr.*;
}

inline fn writeU32(offset: u32, value: u32) void {
    const ptr: *align(1) allowzero u32 = @ptrCast(ptrAt(offset));
    ptr.* = value;
}

inline fn bumpPtr() u32 {
    return readU32(OFFSET_BUMP_PTR);
}

inline fn setBumpPtr(value: u32) void {
    writeU32(OFFSET_BUMP_PTR, value);
}

inline fn freelistHead(size_class: SizeClass) u32 {
    return readU32(freelistOffset(size_class));
}

inline fn setFreelistHead(size_class: SizeClass, value: u32) void {
    writeU32(freelistOffset(size_class), value);
}

// =============================================================================
// Size Classes
// =============================================================================

const SizeClass = enum(u8) {
    span_system = 0,
    col_1b = 1,
    col_4b = 2,
    col_8b = 3,
};

inline fn freelistOffset(sc: SizeClass) u32 {
    return switch (sc) {
        .span_system => OFFSET_FREELIST_SPAN,
        .col_1b => OFFSET_FREELIST_1B,
        .col_4b => OFFSET_FREELIST_4B,
        .col_8b => OFFSET_FREELIST_8B,
    };
}

inline fn blockSize(sc: SizeClass) u32 {
    return switch (sc) {
        .span_system => g_span_system_size,
        .col_1b => g_col_1b_size,
        .col_4b => g_col_4b_size,
        .col_8b => g_col_8b_size,
    };
}

// =============================================================================
// Allocator Core
// =============================================================================

/// Allocate a block from the given size class.
/// Returns offset into WASM memory, or 0 on failure.
fn alloc(sc: SizeClass) u32 {
    const head = freelistHead(sc);

    if (head != 0) {
        // Pop from freelist - read next pointer from block
        const next = readU32(head);
        setFreelistHead(sc, next);

        // Debug: increment alloc count
        writeU32(OFFSET_ALLOC_COUNT, readU32(OFFSET_ALLOC_COUNT) + 1);

        return head;
    }

    // Freelist empty - bump allocate
    const size = blockSize(sc);
    const current_bump = bumpPtr();

    // Align to 8 bytes
    const aligned = (current_bump + 7) & ~@as(u32, 7);
    const new_bump = aligned + size;

    // Check memory bounds (WASM memory is fixed size)
    // TODO: memory.grow() if needed
    // For now, assume sufficient initial memory

    setBumpPtr(new_bump);

    // Debug: increment alloc count
    writeU32(OFFSET_ALLOC_COUNT, readU32(OFFSET_ALLOC_COUNT) + 1);

    return aligned;
}

/// Free a block back to its size class freelist.
fn free(offset: u32, sc: SizeClass) void {
    // Push onto freelist - write current head as next pointer
    const old_head = freelistHead(sc);
    writeU32(offset, old_head);
    setFreelistHead(sc, offset);

    // Debug: increment free count
    writeU32(OFFSET_FREE_COUNT, readU32(OFFSET_FREE_COUNT) + 1);
}

// =============================================================================
// JS Imports (for timestamp calculation)
// =============================================================================

extern "env" fn performanceNow() f64;
extern "env" fn dateNow() f64;

// =============================================================================
// TraceRoot Layout
// =============================================================================
//
// [startWallClockNanos: i64]   offset 0
// [startMonotonicMs: f64]      offset 8
//
// Total: 16 bytes

const TRACE_ROOT_SIZE: u32 = 16;
const TRACE_ROOT_WALL_CLOCK_OFFSET: u32 = 0;
const TRACE_ROOT_MONOTONIC_OFFSET: u32 = 8;

inline fn getTimestampNanos(trace_root_ptr: u32) i64 {
    const base = ptrAt(trace_root_ptr);
    const wall_ptr: *allowzero const i64 = @ptrCast(@alignCast(base + TRACE_ROOT_WALL_CLOCK_OFFSET));
    const mono_ptr: *allowzero const f64 = @ptrCast(@alignCast(base + TRACE_ROOT_MONOTONIC_OFFSET));

    const current_ms = performanceNow();
    const elapsed_ms = current_ms - mono_ptr.*;
    const elapsed_nanos: i64 = @intFromFloat(elapsed_ms * 1_000_000.0);
    return wall_ptr.* + elapsed_nanos;
}

// =============================================================================
// Exported Functions
// =============================================================================

/// Initialize the allocator with a given capacity.
/// Must be called before any allocations.
export fn init(capacity: u32) void {
    g_capacity = capacity;

    // Compute block sizes
    g_null_bitmap_size = (capacity + 7) >> 3;
    g_span_system_size = (capacity * 9 + 4 + 7) & ~@as(u32, 7); // Align to 8
    g_col_1b_size = g_null_bitmap_size + capacity;
    g_col_4b_size = g_null_bitmap_size + capacity * 4;
    g_col_8b_size = g_null_bitmap_size + capacity * 8;

    // Initialize header
    setBumpPtr(HEADER_SIZE); // Start allocations after header
    writeU32(OFFSET_CAPACITY, capacity);
    setFreelistHead(.span_system, 0);
    setFreelistHead(.col_1b, 0);
    setFreelistHead(.col_4b, 0);
    setFreelistHead(.col_8b, 0);
    writeU32(OFFSET_ALLOC_COUNT, 0);
    writeU32(OFFSET_FREE_COUNT, 0);
}

/// Reset all freelists (for testing/benchmarking).
/// WARNING: This leaks all allocated blocks. Use free() for proper cleanup.
export fn reset() void {
    setBumpPtr(HEADER_SIZE);
    setFreelistHead(.span_system, 0);
    setFreelistHead(.col_1b, 0);
    setFreelistHead(.col_4b, 0);
    setFreelistHead(.col_8b, 0);
    writeU32(OFFSET_ALLOC_COUNT, 0);
    writeU32(OFFSET_FREE_COUNT, 0);
}

/// Get current bump pointer (for debugging).
export fn get_bump_ptr() u32 {
    return bumpPtr();
}

/// Get allocation count (for debugging).
export fn get_alloc_count() u32 {
    return readU32(OFFSET_ALLOC_COUNT);
}

/// Get free count (for debugging).
export fn get_free_count() u32 {
    return readU32(OFFSET_FREE_COUNT);
}

/// Get configured capacity.
export fn get_capacity() u32 {
    return g_capacity;
}

/// Get block sizes (for debugging).
export fn get_span_system_size() u32 {
    return g_span_system_size;
}

export fn get_col_1b_size() u32 {
    return g_col_1b_size;
}

export fn get_col_4b_size() u32 {
    return g_col_4b_size;
}

export fn get_col_8b_size() u32 {
    return g_col_8b_size;
}

// =============================================================================
// Block Allocation Exports
// =============================================================================

/// Allocate a Span System block.
/// Returns offset into WASM memory.
export fn alloc_span_system() u32 {
    return alloc(.span_system);
}

/// Free a Span System block.
export fn free_span_system(offset: u32) void {
    free(offset, .span_system);
}

/// Allocate a 1-byte column block (for enum/boolean).
export fn alloc_col_1b() u32 {
    return alloc(.col_1b);
}

/// Free a 1-byte column block.
export fn free_col_1b(offset: u32) void {
    free(offset, .col_1b);
}

/// Allocate a 4-byte column block (for u32/i32/f32).
export fn alloc_col_4b() u32 {
    return alloc(.col_4b);
}

/// Free a 4-byte column block.
export fn free_col_4b(offset: u32) void {
    free(offset, .col_4b);
}

/// Allocate an 8-byte column block (for f64/i64).
export fn alloc_col_8b() u32 {
    return alloc(.col_8b);
}

/// Free an 8-byte column block.
export fn free_col_8b(offset: u32) void {
    free(offset, .col_8b);
}

// =============================================================================
// TraceRoot Operations
// =============================================================================

/// Initialize a TraceRoot at the given offset.
/// Sets wall clock and monotonic start times.
export fn init_trace_root(trace_root_ptr: u32) void {
    const base = ptrAt(trace_root_ptr);

    // Write wall clock start time (i64 at offset 0)
    const wall_ptr: *allowzero i64 = @ptrCast(@alignCast(base + TRACE_ROOT_WALL_CLOCK_OFFSET));
    const ms: i64 = @intFromFloat(dateNow());
    wall_ptr.* = ms * 1_000_000;

    // Write monotonic start time (f64 at offset 8)
    const mono_ptr: *allowzero f64 = @ptrCast(@alignCast(base + TRACE_ROOT_MONOTONIC_OFFSET));
    mono_ptr.* = performanceNow();
}

// =============================================================================
// Span Lifecycle Operations
// =============================================================================

/// Write span-start to a Span System block.
/// Sets row 0 (span-start) and row 1 (span-exception placeholder).
/// Sets _writeIndex = 2.
export fn span_start(system_ptr: u32, trace_root_ptr: u32) void {
    const base = ptrAt(system_ptr);

    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base + getSystemTimestampOffset()));
    const et_ptr: [*]allowzero u8 = base + getSystemEntryTypeOffset();

    // Row 0: span-start
    ts_ptr[0] = getTimestampNanos(trace_root_ptr);
    et_ptr[0] = ENTRY_TYPE_SPAN_START;

    // Row 1: pre-initialize as span-exception
    ts_ptr[1] = 0;
    et_ptr[1] = ENTRY_TYPE_SPAN_EXCEPTION;

    // Set _writeIndex = 2
    const idx_ptr: *allowzero u32 = @ptrCast(@alignCast(base + getSystemWriteIndexOffset()));
    idx_ptr.* = 2;
}

/// Write span-ok to row 1.
export fn span_end_ok(system_ptr: u32, trace_root_ptr: u32) void {
    const base = ptrAt(system_ptr);

    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base + getSystemTimestampOffset()));
    const et_ptr: [*]allowzero u8 = base + getSystemEntryTypeOffset();

    et_ptr[1] = ENTRY_TYPE_SPAN_OK;
    ts_ptr[1] = getTimestampNanos(trace_root_ptr);
}

/// Write span-err to row 1.
export fn span_end_err(system_ptr: u32, trace_root_ptr: u32) void {
    const base = ptrAt(system_ptr);

    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base + getSystemTimestampOffset()));
    const et_ptr: [*]allowzero u8 = base + getSystemEntryTypeOffset();

    et_ptr[1] = ENTRY_TYPE_SPAN_ERR;
    ts_ptr[1] = getTimestampNanos(trace_root_ptr);
}

/// Write a log entry (info/debug/warn/error).
/// Returns the row index where the entry was written.
export fn write_log_entry(system_ptr: u32, trace_root_ptr: u32, entry_type: u8) u32 {
    const base = ptrAt(system_ptr);

    // Read current _writeIndex
    const idx_ptr: *allowzero u32 = @ptrCast(@alignCast(base + getSystemWriteIndexOffset()));
    const idx = idx_ptr.*;

    const ts_ptr: [*]allowzero i64 = @ptrCast(@alignCast(base + getSystemTimestampOffset()));
    const et_ptr: [*]allowzero u8 = base + getSystemEntryTypeOffset();

    // Write timestamp and entry_type at current index
    ts_ptr[idx] = getTimestampNanos(trace_root_ptr);
    et_ptr[idx] = entry_type;

    // Increment _writeIndex
    idx_ptr.* = idx + 1;

    return idx;
}

// =============================================================================
// Column Write Operations
// =============================================================================

/// Write an f64 value to a column.
/// col_offset: offset to the column block (or 0 if not yet allocated)
/// Returns: column offset (same as input if already allocated, or newly allocated)
export fn write_col_f64(col_offset: u32, row_idx: u32, value: f64) u32 {
    var offset = col_offset;
    if (offset == 0) {
        offset = alloc(.col_8b);
    }

    const base = ptrAt(offset);

    // Write value (after null bitmap)
    const val_ptr: *align(1) allowzero f64 = @ptrCast(base + g_null_bitmap_size + row_idx * 8);
    val_ptr.* = value;

    // Set null bit (mark as valid)
    base[row_idx >> 3] |= @as(u8, 1) << @intCast(row_idx & 7);

    return offset;
}

/// Write a u32 value to a column.
export fn write_col_u32(col_offset: u32, row_idx: u32, value: u32) u32 {
    var offset = col_offset;
    if (offset == 0) {
        offset = alloc(.col_4b);
    }

    const base = ptrAt(offset);

    // Write value
    const val_ptr: *align(1) allowzero u32 = @ptrCast(base + g_null_bitmap_size + row_idx * 4);
    val_ptr.* = value;

    // Set null bit
    base[row_idx >> 3] |= @as(u8, 1) << @intCast(row_idx & 7);

    return offset;
}

/// Write a u8 value to a column (for enum).
export fn write_col_u8(col_offset: u32, row_idx: u32, value: u8) u32 {
    var offset = col_offset;
    if (offset == 0) {
        offset = alloc(.col_1b);
    }

    const base = ptrAt(offset);

    // Write value
    base[g_null_bitmap_size + row_idx] = value;

    // Set null bit
    base[row_idx >> 3] |= @as(u8, 1) << @intCast(row_idx & 7);

    return offset;
}

// =============================================================================
// Debug Exports
// =============================================================================

export fn get_performance_now() f64 {
    return performanceNow();
}

export fn debug_compute_timestamp(trace_root_ptr: u32) i64 {
    return getTimestampNanos(trace_root_ptr);
}

/// Read timestamp at given row from a Span System block.
export fn read_timestamp(system_ptr: u32, row_idx: u32) i64 {
    const base = ptrAt(system_ptr);
    const ts_ptr: [*]allowzero const i64 = @ptrCast(@alignCast(base + getSystemTimestampOffset()));
    return ts_ptr[row_idx];
}

/// Read entry_type at given row from a Span System block.
export fn read_entry_type(system_ptr: u32, row_idx: u32) u8 {
    const base = ptrAt(system_ptr);
    const et_ptr: [*]allowzero const u8 = base + getSystemEntryTypeOffset();
    return et_ptr[row_idx];
}

/// Read _writeIndex from a Span System block.
export fn read_write_index(system_ptr: u32) u32 {
    const base = ptrAt(system_ptr);
    const idx_ptr: *allowzero const u32 = @ptrCast(@alignCast(base + getSystemWriteIndexOffset()));
    return idx_ptr.*;
}

/// Read f64 value from a column.
export fn read_col_f64(col_offset: u32, row_idx: u32) f64 {
    const base = ptrAt(col_offset);
    const val_ptr: *align(1) allowzero const f64 = @ptrCast(base + g_null_bitmap_size + row_idx * 8);
    return val_ptr.*;
}

/// Check if null bit is set for a row.
export fn read_col_is_valid(col_offset: u32, row_idx: u32) u8 {
    const base = ptrAt(col_offset);
    const byte = base[row_idx >> 3];
    const bit: u8 = @as(u8, 1) << @intCast(row_idx & 7);
    return if ((byte & bit) != 0) 1 else 0;
}
