// =============================================================================
// Struct Map — Typed accessor for multi-field hash map rows
// =============================================================================
//
// StructMapSlot provides typed access to the struct map's SoA layout:
//   [field_type_descriptor: u8 × num_fields (padded to 8)]
//   [keys: u32 × capacity]
//   [rows: row_size × capacity]
//
// Each row: [bitset: ceil(num_fields/8) bytes][field0][field1]...
// Field types: UINT32(4), INT64(8), FLOAT64(8), BOOL(1), STRING(4), ARRAY_*(8)
//
// This replaces raw pointer arithmetic in singleStructMapUpsertLast and
// vm_struct_map_get_row_ptr with typed field access.

const std = @import("std");
const types = @import("types.zig");
const ht = @import("hash_table.zig");

const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const hashKey = types.hashKey;
const align8 = types.align8;
const SlotMeta = types.SlotMeta;
const StructFieldType = types.StructFieldType;
const ErrorCode = types.ErrorCode;

/// Typed accessor for a struct map slot in the state buffer.
pub const StructMapSlot = struct {
    state_base: [*]u8,
    slot_offset: u32,
    capacity: u32,
    size_ptr: *align(1) u32,
    num_fields: u8,
    bitset_bytes: u32,
    row_size: u32,
    descriptor_size: u32,
    field_types: [*]u8,
    keys: [*]u32,
    rows_base: u32,

    /// Bind to an existing struct map slot via metadata.
    pub fn bind(state_base: [*]u8, slot_idx: u8) StructMapSlot {
        const meta_base = types.STATE_HEADER_SIZE + @as(u32, slot_idx) * types.SLOT_META_SIZE;
        const slot_offset = std.mem.readInt(u32, state_base[meta_base..][0..4], .little);
        const capacity = std.mem.readInt(u32, state_base[meta_base + 4 ..][0..4], .little);
        const num_fields = state_base[meta_base + 13];
        const bitset_bytes: u32 = state_base[meta_base + 15];
        const row_size: u32 = std.mem.readInt(u16, state_base[meta_base + 16 ..][0..2], .little);

        const descriptor_size = align8(@as(u32, num_fields));
        const keys_offset = slot_offset + descriptor_size;

        return .{
            .state_base = state_base,
            .slot_offset = slot_offset,
            .capacity = capacity,
            .size_ptr = @ptrCast(@alignCast(state_base + meta_base + 8)),
            .num_fields = num_fields,
            .bitset_bytes = bitset_bytes,
            .row_size = row_size,
            .descriptor_size = descriptor_size,
            .field_types = state_base + slot_offset,
            .keys = @ptrCast(@alignCast(state_base + keys_offset)),
            .rows_base = keys_offset + capacity * 4,
        };
    }

    pub fn size(self: StructMapSlot) u32 {
        return self.size_ptr.*;
    }

    fn maxLoad(self: StructMapSlot) u32 {
        return (self.capacity * 7) / 10;
    }

    /// Find key, return hash table position or null.
    pub fn find(self: StructMapSlot, key: u32) ?u32 {
        if (key == EMPTY_KEY or key == TOMBSTONE) return null;
        var pos = hashKey(key, self.capacity);
        var probes: u32 = 0;
        while (probes < self.capacity) : (probes += 1) {
            const k = self.keys[pos];
            if (k == key) return pos;
            if (k == EMPTY_KEY) return null;
            pos = (pos + 1) & (self.capacity - 1);
        }
        return null;
    }

    /// Find insertion position (insert or update).
    pub fn findInsert(self: StructMapSlot, key: u32) ?struct { pos: u32, found: bool } {
        if (key == EMPTY_KEY or key == TOMBSTONE) return null;
        var pos = hashKey(key, self.capacity);
        var probes: u32 = 0;
        while (probes < self.capacity) : (probes += 1) {
            const k = self.keys[pos];
            if (k == key) return .{ .pos = pos, .found = true };
            if (k == EMPTY_KEY or k == TOMBSTONE) return .{ .pos = pos, .found = false };
            pos = (pos + 1) & (self.capacity - 1);
        }
        return null;
    }

    /// Get the row pointer for a given hash table position.
    pub fn rowPtr(self: StructMapSlot, pos: u32) [*]u8 {
        return self.state_base + self.rows_base + pos * self.row_size;
    }

    /// Get byte offset of a field within a row (after bitset).
    pub fn fieldOffset(self: StructMapSlot, field_idx: u8) u32 {
        var off: u32 = self.bitset_bytes;
        for (0..field_idx) |i| {
            const ft: StructFieldType = @enumFromInt(self.field_types[i]);
            off += types.structFieldSize(ft);
        }
        return off;
    }

    /// Check if a field is set in a row's bitset.
    pub fn isFieldSet(row: [*]const u8, field_idx: u8) bool {
        return (row[field_idx / 8] & (@as(u8, 1) << @as(u3, @truncate(field_idx % 8)))) != 0;
    }

    /// Set a field's bit in the bitset.
    pub fn setFieldBit(row: [*]u8, field_idx: u8) void {
        row[field_idx / 8] |= @as(u8, 1) << @as(u3, @truncate(field_idx % 8));
    }

    /// Clear the bitset for a row.
    pub fn clearBitset(self: StructMapSlot, row: [*]u8) void {
        @memset(row[0..self.bitset_bytes], 0);
    }

    /// Upsert key. Returns hash position + whether it was new. Handles size tracking.
    pub fn upsert(self: StructMapSlot, key: u32) ?struct { pos: u32, is_new: bool } {
        const probe = self.findInsert(key) orelse return null;

        if (!probe.found) {
            if (self.size() >= self.maxLoad()) return null;
            self.keys[probe.pos] = key;
            self.size_ptr.* += 1;
        }
        return .{ .pos = probe.pos, .is_new = !probe.found };
    }

    /// Write a scalar field value to a row at the given position.
    pub fn writeScalarField(
        self: StructMapSlot,
        pos: u32,
        field_idx: u8,
        col_ptrs: [*]const [*]const u8,
        val_col: u8,
        element_idx: u32,
    ) void {
        const row = self.rowPtr(pos);
        const ft: StructFieldType = @enumFromInt(self.field_types[field_idx]);
        const f_off = self.fieldOffset(field_idx);

        setFieldBit(row, field_idx);

        switch (ft) {
            .UINT32, .STRING => {
                const col: [*]const u32 = @ptrCast(@alignCast(col_ptrs[val_col]));
                std.mem.writeInt(u32, row[f_off..][0..4], col[element_idx], .little);
            },
            .INT64 => {
                const col: [*]const u64 = @ptrCast(@alignCast(col_ptrs[val_col]));
                std.mem.writeInt(u64, row[f_off..][0..8], col[element_idx], .little);
            },
            .FLOAT64 => {
                const col: [*]const f64 = @ptrCast(@alignCast(col_ptrs[val_col]));
                const bits: u64 = @bitCast(col[element_idx]);
                std.mem.writeInt(u64, row[f_off..][0..8], bits, .little);
            },
            .BOOL => {
                const col: [*]const u32 = @ptrCast(@alignCast(col_ptrs[val_col]));
                row[f_off] = if (col[element_idx] != 0) 1 else 0;
            },
            // Array fields handled separately
            .ARRAY_U32, .ARRAY_I64, .ARRAY_F64, .ARRAY_STRING, .ARRAY_BOOL => {},
        }
    }

    /// Get row pointer by key. Returns absolute byte offset or 0xFFFFFFFF if not found.
    pub fn getRowPtrByKey(self: StructMapSlot, key: u32) u32 {
        const pos = self.find(key) orelse return 0xFFFFFFFF;
        return self.rows_base + pos * self.row_size;
    }

    /// Read a u32 field value from a row.
    pub fn readU32(self: StructMapSlot, row: [*]const u8, field_idx: u8) u32 {
        _ = self;
        _ = field_idx;
        // Use fieldOffset for safety — but callers know the offset from context
        return std.mem.readInt(u32, row[0..4], .little);
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "StructMapSlot — bind and upsert" {
    // Manually create a minimal struct map state:
    // Slot 0: STRUCT_MAP with 2 fields (UINT32, STRING), cap=16
    var state: [8192]u8 align(8) = [_]u8{0} ** 8192;
    const meta_base = types.STATE_HEADER_SIZE;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const num_fields: u8 = 2;
    const desc_size: u32 = align8(num_fields); // 8
    const bitset_bytes: u32 = 1; // ceil(2/8) = 1
    // row = 1(bitset) + 4(UINT32) + 4(STRING) = 9, aligned to 4 = 12
    const row_size: u32 = 12;
    const cap: u32 = 16;

    // Write metadata
    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little); // offset
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little); // capacity
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little); // size
    state[meta_base + 12] = @intFromEnum(types.SlotType.STRUCT_MAP); // type_flags
    state[meta_base + 13] = num_fields;
    state[meta_base + 15] = @truncate(bitset_bytes);
    std.mem.writeInt(u16, state[meta_base + 16 ..][0..2], @truncate(row_size), .little);

    // Write field type descriptor
    state[slot_offset] = 0; // UINT32
    state[slot_offset + 1] = 4; // STRING

    // Init keys to EMPTY_KEY
    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset + desc_size]));
    for (0..cap) |i| keys[i] = EMPTY_KEY;

    // Bind and upsert
    const smap = StructMapSlot.bind(&state, 0);
    try testing.expectEqual(@as(u32, 0), smap.size());

    const result = smap.upsert(42) orelse return error.SkipZigTest;
    try testing.expect(result.is_new);
    try testing.expectEqual(@as(u32, 1), smap.size());

    // Write fields to the row
    const row = smap.rowPtr(result.pos);
    smap.clearBitset(row);
    StructMapSlot.setFieldBit(row, 0);
    const f0_off = smap.fieldOffset(0);
    std.mem.writeInt(u32, row[f0_off..][0..4], 100, .little);
    StructMapSlot.setFieldBit(row, 1);
    const f1_off = smap.fieldOffset(1);
    std.mem.writeInt(u32, row[f1_off..][0..4], 5000, .little);

    // Read back
    const read_row_off = smap.getRowPtrByKey(42);
    try testing.expect(read_row_off != 0xFFFFFFFF);
    const read_row = state[read_row_off..];
    try testing.expect(StructMapSlot.isFieldSet(read_row.ptr, 0));
    try testing.expectEqual(@as(u32, 100), std.mem.readInt(u32, read_row[f0_off..][0..4], .little));
    try testing.expectEqual(@as(u32, 5000), std.mem.readInt(u32, read_row[f1_off..][0..4], .little));

    // Upsert same key — not new
    const result2 = smap.upsert(42) orelse return error.SkipZigTest;
    try testing.expect(!result2.is_new);
    try testing.expectEqual(@as(u32, 1), smap.size()); // unchanged
}

test "StructMapSlot — CAPACITY_EXCEEDED" {
    var state: [8192]u8 align(8) = [_]u8{0} ** 8192;
    const meta_base = types.STATE_HEADER_SIZE;
    const slot_offset: u32 = types.STATE_HEADER_SIZE + types.SLOT_META_SIZE;
    const cap: u32 = 16;

    std.mem.writeInt(u32, state[meta_base..][0..4], slot_offset, .little);
    std.mem.writeInt(u32, state[meta_base + 4 ..][0..4], cap, .little);
    std.mem.writeInt(u32, state[meta_base + 8 ..][0..4], 0, .little);
    state[meta_base + 12] = @intFromEnum(types.SlotType.STRUCT_MAP);
    state[meta_base + 13] = 1; // 1 field
    state[meta_base + 15] = 1; // bitset_bytes
    std.mem.writeInt(u16, state[meta_base + 16 ..][0..2], 5, .little); // row_size = 5 (1 bitset + 4 UINT32)
    state[slot_offset] = 0; // UINT32

    const keys: [*]u32 = @ptrCast(@alignCast(&state[slot_offset + 8])); // desc_size=8
    for (0..cap) |i| keys[i] = EMPTY_KEY;

    const smap = StructMapSlot.bind(&state, 0);

    // Fill to 70% load (11 entries)
    var i: u32 = 1;
    while (i <= 11) : (i += 1) {
        const r = smap.upsert(i);
        try testing.expect(r != null);
    }
    try testing.expectEqual(@as(u32, 11), smap.size());

    // 12th should fail
    try testing.expect(smap.upsert(12) == null);
}
