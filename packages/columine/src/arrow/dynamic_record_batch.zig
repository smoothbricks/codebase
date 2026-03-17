//! Dynamic RecordBatch encoder for variable field count
//!
//! Extends Phase 7's record_batch.zig to support dynamic schemas.
//! The key difference: buffer count and field count are determined at runtime
//! from DynamicSchemaConfig instead of being hardcoded.
//!
//! NOTE: This module provides the framework for dynamic RecordBatch encoding.
//! The actual FlatBuffer metadata generation still uses Flechette templates
//! (same approach as Phase 7), but with dynamic patching based on field count.

const std = @import("std");
const dynamic_schema = @import("dynamic_schema");

/// Continuation marker for Arrow IPC messages
pub const CONTINUATION_MARKER: u32 = 0xFFFFFFFF;

/// Buffer descriptor (offset + length) for Arrow IPC
pub const BufferDesc = struct {
    offset: i64,
    length: i64,
};

/// Field node (length + null_count) for Arrow IPC
pub const FieldNode = struct {
    length: i64,
    null_count: i64,
};

/// Align a size to 8-byte boundary (Arrow IPC requirement)
pub fn alignTo8(size: usize) usize {
    return (size + 7) & ~@as(usize, 7);
}

/// Write padding bytes (zeros) for alignment
pub fn writePadding(buffer: []u8, offset: usize, count: usize) void {
    if (count > 0 and offset + count <= buffer.len) {
        @memset(buffer[offset..][0..count], 0);
    }
}

/// Compute buffer count for an Arrow schema.
/// Each field contributes buffers based on type and nullability.
///
/// This function duplicates the logic in SignalSchemaField.bufferCount()
/// but works with a slice for convenience.
pub fn computeBufferCount(fields: []const dynamic_schema.SignalSchemaField) u32 {
    var count: u32 = 0;
    for (fields) |field| {
        count += field.bufferCount();
    }
    return count;
}

/// Dynamic column data for RecordBatch encoding.
/// Represents a single column's data buffers.
pub const DynamicColumn = struct {
    /// Field index in schema
    field_idx: u32,
    /// Arrow type
    arrow_type: dynamic_schema.ArrowType,
    /// Is this column nullable?
    nullable: bool,
    /// Validity bitmap (null if all values present, non-null for nullable columns)
    validity: ?[]const u8,
    /// Data buffer (always present except for Null type)
    data: []const u8,
    /// Offsets buffer (for variable-length types like Utf8, Binary)
    offsets: ?[]const u8,

    /// Create a column for Utf8 data
    pub fn utf8(
        field_idx: u32,
        nullable: bool,
        validity: ?[]const u8,
        offsets: []const u8,
        data: []const u8,
    ) DynamicColumn {
        return .{
            .field_idx = field_idx,
            .arrow_type = .Utf8,
            .nullable = nullable,
            .validity = validity,
            .data = data,
            .offsets = offsets,
        };
    }

    /// Create a column for Binary data
    pub fn binary(
        field_idx: u32,
        nullable: bool,
        validity: ?[]const u8,
        offsets: []const u8,
        data: []const u8,
    ) DynamicColumn {
        return .{
            .field_idx = field_idx,
            .arrow_type = .Binary,
            .nullable = nullable,
            .validity = validity,
            .data = data,
            .offsets = offsets,
        };
    }

    /// Create a column for Int64 data
    pub fn int64(
        field_idx: u32,
        nullable: bool,
        validity: ?[]const u8,
        data: []const u8,
    ) DynamicColumn {
        return .{
            .field_idx = field_idx,
            .arrow_type = .Int32,
            .nullable = nullable,
            .validity = validity,
            .data = data,
            .offsets = null,
        };
    }

    /// Create a column for Float64 data
    pub fn float64(
        field_idx: u32,
        nullable: bool,
        validity: ?[]const u8,
        data: []const u8,
    ) DynamicColumn {
        return .{
            .field_idx = field_idx,
            .arrow_type = .Float64,
            .nullable = nullable,
            .validity = validity,
            .data = data,
            .offsets = null,
        };
    }

    /// Create a column for Bool data
    pub fn boolean(
        field_idx: u32,
        nullable: bool,
        validity: ?[]const u8,
        data: []const u8,
    ) DynamicColumn {
        return .{
            .field_idx = field_idx,
            .arrow_type = .Bool,
            .nullable = nullable,
            .validity = validity,
            .data = data,
            .offsets = null,
        };
    }
};

/// Dynamic body builder for variable-field RecordBatch.
/// Similar to ipc_writer.BodyBuilder but tracks per-column buffers.
pub const DynamicBodyBuilder = struct {
    buffer: []u8,
    offset: usize,
    buffer_descs: [64]BufferDesc, // Support up to 64 buffers
    buffer_desc_count: usize,
    field_nodes: [32]FieldNode, // Support up to 32 fields
    field_node_count: usize,

    const Self = @This();
    const MAX_BUFFERS = 64;
    const MAX_FIELDS = 32;

    /// Initialize with output buffer
    pub fn init(buffer: []u8) Self {
        return .{
            .buffer = buffer,
            .offset = 0,
            .buffer_descs = undefined,
            .buffer_desc_count = 0,
            .field_nodes = undefined,
            .field_node_count = 0,
        };
    }

    /// Add a column to the body, generating appropriate buffers and field node.
    /// Returns true on success, false if buffer too small.
    pub fn addColumn(
        self: *Self,
        column: DynamicColumn,
        row_count: i64,
        null_count: i64,
    ) bool {
        // Track field node
        if (self.field_node_count >= MAX_FIELDS) return false;
        self.field_nodes[self.field_node_count] = .{
            .length = row_count,
            .null_count = null_count,
        };
        self.field_node_count += 1;

        // Add validity buffer (always present per Flechette convention)
        // Non-nullable columns have empty validity buffer
        if (column.validity) |validity| {
            if (!self.addBuffer(validity)) return false;
        } else {
            if (!self.addEmptyBuffer()) return false;
        }

        // Add type-specific buffers
        switch (column.arrow_type) {
            .Utf8, .Binary => {
                // Offsets buffer (required for variable-length)
                if (column.offsets) |offsets| {
                    if (!self.addBuffer(offsets)) return false;
                } else {
                    return false; // Variable-length types require offsets
                }
                // Data buffer
                if (!self.addBuffer(column.data)) return false;
            },
            .Int32, .Int64, .Float64, .Bool => {
                // Just data buffer
                if (!self.addBuffer(column.data)) return false;
            },
            .Null => {
                // No data buffer for Null type
            },
        }

        return true;
    }

    /// Add a data buffer to the body with 8-byte alignment
    fn addBuffer(self: *Self, data: []const u8) bool {
        const aligned_len = alignTo8(data.len);

        if (self.offset + aligned_len > self.buffer.len) {
            return false;
        }

        if (self.buffer_desc_count >= MAX_BUFFERS) return false;

        // Record descriptor
        self.buffer_descs[self.buffer_desc_count] = .{
            .offset = @intCast(self.offset),
            .length = @intCast(data.len),
        };
        self.buffer_desc_count += 1;

        // Copy data
        @memcpy(self.buffer[self.offset..][0..data.len], data);

        // Zero-fill padding
        const padding = aligned_len - data.len;
        if (padding > 0) {
            @memset(self.buffer[self.offset + data.len ..][0..padding], 0);
        }

        self.offset += aligned_len;
        return true;
    }

    /// Add an empty buffer (for non-nullable columns' validity)
    fn addEmptyBuffer(self: *Self) bool {
        if (self.buffer_desc_count >= MAX_BUFFERS) return false;

        self.buffer_descs[self.buffer_desc_count] = .{
            .offset = @intCast(self.offset),
            .length = 0,
        };
        self.buffer_desc_count += 1;

        return true;
    }

    /// Get current body length
    pub fn bodyLength(self: *const Self) usize {
        return self.offset;
    }

    /// Get body bytes
    pub fn getBody(self: *const Self) []const u8 {
        return self.buffer[0..self.offset];
    }

    /// Get all buffer descriptors
    pub fn getBufferDescs(self: *const Self) []const BufferDesc {
        return self.buffer_descs[0..self.buffer_desc_count];
    }

    /// Get all field nodes
    pub fn getFieldNodes(self: *const Self) []const FieldNode {
        return self.field_nodes[0..self.field_node_count];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "computeBufferCount for 4-field schema" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id: validity + offsets + data = 3
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type: validity + offsets + data = 3
        .{ .arrow_type = .Int64, .nullable = 0 }, // timestamp: validity + data = 2
        .{ .arrow_type = .Binary, .nullable = 1 }, // value: validity + offsets + data = 3
    };

    // Total: 3 + 3 + 2 + 3 = 11 (matches Phase 7)
    const count = computeBufferCount(&fields);
    try std.testing.expectEqual(@as(u32, 11), count);
}

test "computeBufferCount for extended schema" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id: 3
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type: 3
        .{ .arrow_type = .Int64, .nullable = 0 }, // timestamp: 2
        .{ .arrow_type = .Utf8, .nullable = 1 }, // value.orderId: 3
        .{ .arrow_type = .Float64, .nullable = 1 }, // value.amount: 2
        .{ .arrow_type = .Bool, .nullable = 1 }, // value.confirmed: 2
    };

    // Total: 3 + 3 + 2 + 3 + 2 + 2 = 15
    const count = computeBufferCount(&fields);
    try std.testing.expectEqual(@as(u32, 15), count);
}

test "DynamicColumn constructors" {
    const col = DynamicColumn.utf8(0, false, null, &[_]u8{}, &[_]u8{});
    try std.testing.expectEqual(@as(u32, 0), col.field_idx);
    try std.testing.expectEqual(dynamic_schema.ArrowType.Utf8, col.arrow_type);
    try std.testing.expect(!col.nullable);

    const int_col = DynamicColumn.int64(2, false, null, &[_]u8{});
    try std.testing.expectEqual(dynamic_schema.ArrowType.Int32, int_col.arrow_type);
    try std.testing.expect(int_col.offsets == null);
}

test "DynamicBodyBuilder addColumn Utf8" {
    var buffer: [256]u8 = undefined;
    var builder = DynamicBodyBuilder.init(&buffer);

    // Simulate single-row Utf8 column: "hello"
    const offsets = [_]u8{ 0, 0, 0, 0, 5, 0, 0, 0 }; // [0, 5] as i32
    const data = "hello";

    const col = DynamicColumn.utf8(0, false, null, &offsets, data);
    const ok = builder.addColumn(col, 1, 0);
    try std.testing.expect(ok);

    // Should have 3 buffers: validity (empty), offsets, data
    try std.testing.expectEqual(@as(usize, 3), builder.buffer_desc_count);

    // Check validity buffer (empty)
    try std.testing.expectEqual(@as(i64, 0), builder.buffer_descs[0].length);

    // Check offsets buffer
    try std.testing.expectEqual(@as(i64, 8), builder.buffer_descs[1].length);

    // Check data buffer
    try std.testing.expectEqual(@as(i64, 5), builder.buffer_descs[2].length);

    // Should have 1 field node
    try std.testing.expectEqual(@as(usize, 1), builder.field_node_count);
    try std.testing.expectEqual(@as(i64, 1), builder.field_nodes[0].length);
    try std.testing.expectEqual(@as(i64, 0), builder.field_nodes[0].null_count);
}

test "DynamicBodyBuilder addColumn Int64" {
    var buffer: [256]u8 = undefined;
    var builder = DynamicBodyBuilder.init(&buffer);

    // Simulate single-row Int64 column: 12345
    const data = std.mem.toBytes(@as(i64, 12345));

    const col = DynamicColumn.int64(0, false, null, &data);
    const ok = builder.addColumn(col, 1, 0);
    try std.testing.expect(ok);

    // Should have 2 buffers: validity (empty), data
    try std.testing.expectEqual(@as(usize, 2), builder.buffer_desc_count);

    // Check validity buffer (empty)
    try std.testing.expectEqual(@as(i64, 0), builder.buffer_descs[0].length);

    // Check data buffer
    try std.testing.expectEqual(@as(i64, 8), builder.buffer_descs[1].length);
}

test "DynamicBodyBuilder alignment" {
    var buffer: [256]u8 = undefined;
    var builder = DynamicBodyBuilder.init(&buffer);

    // Add 5-byte data (should be padded to 8)
    const offsets = [_]u8{ 0, 0, 0, 0, 5, 0, 0, 0 };
    const data = "hello"; // 5 bytes

    const col = DynamicColumn.utf8(0, false, null, &offsets, data);
    _ = builder.addColumn(col, 1, 0);

    // Body length should be 0 + 8 + 8 = 16 (empty validity + offsets + padded data)
    try std.testing.expectEqual(@as(usize, 16), builder.bodyLength());

    // Add another column - should start at aligned offset
    const offsets2 = [_]u8{ 0, 0, 0, 0, 3, 0, 0, 0 };
    const data2 = "abc";

    const col2 = DynamicColumn.utf8(1, false, null, &offsets2, data2);
    _ = builder.addColumn(col2, 1, 0);

    // Second column's offsets should start at offset 16
    try std.testing.expectEqual(@as(i64, 16), builder.buffer_descs[4].offset);
}

// =============================================================================
// Dynamic RecordBatch Encoder
// =============================================================================

/// Encode a RecordBatch message with dynamic field/buffer counts.
///
/// Unlike record_batch.encodeRecordBatch which uses a fixed 4-field template,
/// this function builds FlatBuffer metadata dynamically based on the actual
/// field and buffer counts from DynamicBodyBuilder.
///
/// Arrow IPC RecordBatch message layout:
/// [continuation: 0xFFFFFFFF][metadata_size: u32][FlatBuffer Message][body]
///
/// Parameters:
/// - output: Buffer to write the complete message to
/// - row_count: Number of rows in this batch
/// - body_builder: Builder containing field nodes, buffer descriptors, and body data
/// - body_length: Total length of body data (should match body_builder.bodyLength())
///
/// Returns: Total bytes written (header + metadata + body), or 0 on error
pub fn encodeRecordBatchDynamic(
    output: []u8,
    row_count: i64,
    body_builder: *const DynamicBodyBuilder,
    body_length: usize,
) usize {
    const field_nodes = body_builder.getFieldNodes();
    const buffer_descs = body_builder.getBufferDescs();
    const body = body_builder.getBody();

    const field_count: u32 = @intCast(field_nodes.len);
    const buffer_count: u32 = @intCast(buffer_descs.len);

    // Compute FlatBuffer metadata size:
    // The RecordBatch FlatBuffer structure (simplified):
    //   - Root offset (4 bytes)
    //   - Message vtable (12 bytes)
    //   - Message table (variable, ~20 bytes base)
    //   - RecordBatch vtable (10 bytes)
    //   - RecordBatch table (variable, ~12 bytes base)
    //   - Buffers vector: 4 + (buffer_count * 16) bytes
    //   - Nodes vector: 4 + (field_count * 16) bytes
    //
    // Using formula similar to record_batch.zig template:
    // Base overhead ~72 bytes + vectors
    const base_overhead: usize = 76; // Vtables, root, tables, padding
    const buffers_vec_size: usize = 4 + @as(usize, buffer_count) * 16;
    const nodes_vec_size: usize = 4 + @as(usize, field_count) * 16;
    const metadata_unaligned: usize = base_overhead + buffers_vec_size + nodes_vec_size;
    const metadata_size: usize = alignTo8(metadata_unaligned);

    // Total output size needed
    const total_size: usize = 8 + metadata_size + body_length; // 8 = continuation + metadata_size
    if (output.len < total_size) return 0;

    var write_offset: usize = 0;

    // Write continuation marker
    std.mem.writeInt(u32, output[write_offset..][0..4], CONTINUATION_MARKER, .little);
    write_offset += 4;

    // Write metadata size
    std.mem.writeInt(u32, output[write_offset..][0..4], @intCast(metadata_size), .little);
    write_offset += 4;

    const metadata_start = write_offset;

    // Build FlatBuffer in place
    // We'll construct the FlatBuffer from the bottom up (vectors first, then tables)
    // and write it all at the end.

    // Clear metadata area
    @memset(output[metadata_start..][0..metadata_size], 0);

    // FlatBuffer layout (offsets relative to metadata_start):
    // 0: Root offset (points to Message table)
    // 8: Message vtable
    // 20: Message table
    // 42: RecordBatch vtable
    // 52: RecordBatch table start
    // 76: Buffers vector (4 bytes count + buffer_count * 16)
    // 76 + buffers_vec_size: Nodes vector (4 bytes count + field_count * 16)

    // Root offset: points to Message table at offset 20
    std.mem.writeInt(u32, output[metadata_start..][0..4], 20, .little);

    // Message vtable (at offset 8)
    // vtable: [vtable_size:u16][table_size:u16][field_offsets...]
    const msg_vtable_offset: usize = 8;
    std.mem.writeInt(u16, output[metadata_start + msg_vtable_offset ..][0..2], 12, .little); // vtable size
    std.mem.writeInt(u16, output[metadata_start + msg_vtable_offset + 2 ..][0..2], 22, .little); // table size
    std.mem.writeInt(u16, output[metadata_start + msg_vtable_offset + 4 ..][0..2], 20, .little); // version offset
    std.mem.writeInt(u16, output[metadata_start + msg_vtable_offset + 6 ..][0..2], 19, .little); // header_type offset
    std.mem.writeInt(u16, output[metadata_start + msg_vtable_offset + 8 ..][0..2], 12, .little); // header offset
    std.mem.writeInt(u16, output[metadata_start + msg_vtable_offset + 10 ..][0..2], 4, .little); // bodyLength offset

    // Message table (at offset 20)
    // soffset to vtable (negative relative offset)
    std.mem.writeInt(i32, output[metadata_start + 20 ..][0..4], 12, .little); // vtable at 20-12=8

    // bodyLength at offset 20+4=24 (i64)
    std.mem.writeInt(i64, output[metadata_start + 24 ..][0..8], @intCast(body_length), .little);

    // header offset at 20+12=32 (offset to RecordBatch table, relative)
    std.mem.writeInt(u32, output[metadata_start + 32 ..][0..4], 20, .little); // RecordBatch at 52, relative from 32: 52-32=20

    // version at 20+20=40 (u16) = 4 (IPC v4)
    std.mem.writeInt(u16, output[metadata_start + 40 ..][0..2], 4, .little);

    // header_type at 20+19=39 (u8) = 3 (RecordBatch)
    output[metadata_start + 39] = 3;

    // RecordBatch vtable (at offset 42)
    const rb_vtable_offset: usize = 42;
    std.mem.writeInt(u16, output[metadata_start + rb_vtable_offset ..][0..2], 10, .little); // vtable size
    std.mem.writeInt(u16, output[metadata_start + rb_vtable_offset + 2 ..][0..2], 24, .little); // table size
    std.mem.writeInt(u16, output[metadata_start + rb_vtable_offset + 4 ..][0..2], 12, .little); // length offset
    std.mem.writeInt(u16, output[metadata_start + rb_vtable_offset + 6 ..][0..2], 8, .little); // nodes offset
    std.mem.writeInt(u16, output[metadata_start + rb_vtable_offset + 8 ..][0..2], 4, .little); // buffers offset

    // RecordBatch table (at offset 52)
    const rb_table_offset: usize = 52;
    // soffset to vtable (negative relative offset)
    std.mem.writeInt(i32, output[metadata_start + rb_table_offset ..][0..4], 10, .little); // vtable at 52-10=42

    // buffers offset at 52+4=56 (relative offset to buffers vector)
    const buffers_vector_offset: usize = 76;
    std.mem.writeInt(u32, output[metadata_start + 56 ..][0..4], @intCast(buffers_vector_offset - 56), .little);

    // nodes offset at 52+8=60 (relative offset to nodes vector)
    const nodes_vector_offset: usize = buffers_vector_offset + buffers_vec_size;
    std.mem.writeInt(u32, output[metadata_start + 60 ..][0..4], @intCast(nodes_vector_offset - 60), .little);

    // length (row_count) at 52+12=64 (i64)
    std.mem.writeInt(i64, output[metadata_start + 64 ..][0..8], row_count, .little);

    // Buffers vector (at buffers_vector_offset)
    std.mem.writeInt(u32, output[metadata_start + buffers_vector_offset ..][0..4], buffer_count, .little);
    var buf_data_offset = buffers_vector_offset + 4;
    for (buffer_descs) |buf| {
        std.mem.writeInt(i64, output[metadata_start + buf_data_offset ..][0..8], buf.offset, .little);
        buf_data_offset += 8;
        std.mem.writeInt(i64, output[metadata_start + buf_data_offset ..][0..8], buf.length, .little);
        buf_data_offset += 8;
    }

    // Nodes vector (at nodes_vector_offset)
    std.mem.writeInt(u32, output[metadata_start + nodes_vector_offset ..][0..4], field_count, .little);
    var node_data_offset = nodes_vector_offset + 4;
    for (field_nodes) |node| {
        std.mem.writeInt(i64, output[metadata_start + node_data_offset ..][0..8], node.length, .little);
        node_data_offset += 8;
        std.mem.writeInt(i64, output[metadata_start + node_data_offset ..][0..8], node.null_count, .little);
        node_data_offset += 8;
    }

    write_offset += metadata_size;

    // Copy body
    @memcpy(output[write_offset..][0..body_length], body[0..body_length]);
    write_offset += body_length;

    return write_offset;
}

test "encodeRecordBatchDynamic basic" {
    var body_buffer: [256]u8 = undefined;
    var builder = DynamicBodyBuilder.init(&body_buffer);

    // Add a single Utf8 column
    const offsets = [_]u8{ 0, 0, 0, 0, 5, 0, 0, 0 };
    const data = "hello";
    const col = DynamicColumn.utf8(0, false, null, &offsets, data);
    try std.testing.expect(builder.addColumn(col, 1, 0));

    // Encode RecordBatch
    var output: [1024]u8 = undefined;
    const result = encodeRecordBatchDynamic(&output, 1, &builder, builder.bodyLength());

    // Verify continuation marker (first 4 bytes)
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), std.mem.readInt(u32, output[0..4], .little));

    // Verify output length > 0 (metadata + body)
    try std.testing.expect(result > 0);

    // Verify non-trivial output (continuation marker + metadata size + body)
    // Continuation: 4 bytes, metadata_size: 4 bytes, plus body data
    try std.testing.expect(result > 8);
}

test "encodeRecordBatchDynamic 4-field schema" {
    var body_buffer: [512]u8 = undefined;
    var builder = DynamicBodyBuilder.init(&body_buffer);

    // Add 4 columns like our event schema
    // Column 0: id (Utf8)
    const id_offsets = [_]u8{ 0, 0, 0, 0, 7, 0, 0, 0 };
    const id_data = "test-id";
    try std.testing.expect(builder.addColumn(DynamicColumn.utf8(0, false, null, &id_offsets, id_data), 1, 0));

    // Column 1: type (Utf8)
    const type_offsets = [_]u8{ 0, 0, 0, 0, 5, 0, 0, 0 };
    const type_data = "click";
    try std.testing.expect(builder.addColumn(DynamicColumn.utf8(1, false, null, &type_offsets, type_data), 1, 0));

    // Column 2: timestamp (Int64)
    const ts_data = std.mem.toBytes(@as(i64, 1705315800000000));
    try std.testing.expect(builder.addColumn(DynamicColumn.int64(2, false, null, &ts_data), 1, 0));

    // Column 3: value (Binary, nullable with null)
    const value_offsets = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 }; // empty value
    const validity = [_]u8{0}; // null
    try std.testing.expect(builder.addColumn(DynamicColumn.binary(3, true, &validity, &value_offsets, ""), 1, 1));

    // Encode RecordBatch
    var output: [2048]u8 = undefined;
    const result = encodeRecordBatchDynamic(&output, 1, &builder, builder.bodyLength());

    // Verify continuation marker
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), std.mem.readInt(u32, output[0..4], .little));

    // Verify metadata size is reasonable (should be around 200-300 bytes for 4 fields)
    const metadata_size = std.mem.readInt(u32, output[4..8], .little);
    try std.testing.expect(metadata_size > 100);
    try std.testing.expect(metadata_size < 400);

    // Verify output length = 8 + metadata + body
    const body_len = builder.bodyLength();
    try std.testing.expectEqual(8 + metadata_size + body_len, result);
}
