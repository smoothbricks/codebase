//! Arrow IPC stream writer
//!
//! Writes complete Arrow IPC stream: schema + record batches + EOS.
//! Uses dynamic schema configuration from TypeScript.
//!
//! Arrow IPC stream format:
//! [Schema message]
//! [RecordBatch message 1]
//! [RecordBatch message 2]
//! ...
//! [EOS marker: 0xFFFFFFFF 0x00000000]

const std = @import("std");
const columns = @import("../parsing/columns.zig");
const dynamic_schema = @import("dynamic_schema.zig");
const dynamic_record_batch = @import("dynamic_record_batch.zig");

// Re-export commonly used types from dynamic_record_batch
pub const BufferDesc = dynamic_record_batch.BufferDesc;
pub const FieldNode = dynamic_record_batch.FieldNode;
pub const alignTo8 = dynamic_record_batch.alignTo8;

/// End-of-stream marker (8 bytes: continuation + zero metadata size)
pub const EOS_MARKER = [8]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00 };

/// IPC stream writer
/// Manages writing schema, record batches, and EOS marker to a buffer.
pub const IpcWriter = struct {
    buffer: []u8,
    offset: usize,
    schema_written: bool,

    const Self = @This();

    /// Initialize writer with output buffer
    pub fn init(buffer: []u8) Self {
        return .{
            .buffer = buffer,
            .offset = 0,
            .schema_written = false,
        };
    }

    /// Write end-of-stream marker
    /// Returns true if successful, false if buffer too small
    pub fn writeEOS(self: *Self) bool {
        if (self.buffer.len - self.offset < EOS_MARKER.len) return false;

        @memcpy(self.buffer[self.offset..][0..EOS_MARKER.len], &EOS_MARKER);
        self.offset += EOS_MARKER.len;
        return true;
    }

    /// Get total bytes written
    pub fn bytesWritten(self: *const Self) usize {
        return self.offset;
    }

    /// Get output slice (valid IPC bytes so far)
    pub fn getOutput(self: *const Self) []const u8 {
        return self.buffer[0..self.offset];
    }

    /// Check if schema has been written
    pub fn hasSchema(self: *const Self) bool {
        return self.schema_written;
    }

    /// Get remaining buffer capacity
    pub fn remainingCapacity(self: *const Self) usize {
        return self.buffer.len - self.offset;
    }
};

/// Build body buffer from columnar data
/// This is a helper for constructing the body from individual column buffers.
pub const BodyBuilder = struct {
    buffer: []u8,
    offset: usize,
    buffer_descs: [32]BufferDesc,
    buffer_desc_count: usize,

    const Self = @This();
    const MAX_BUFFERS = 32;

    /// Initialize with output buffer
    pub fn init(buffer: []u8) Self {
        return .{
            .buffer = buffer,
            .offset = 0,
            .buffer_descs = undefined,
            .buffer_desc_count = 0,
        };
    }

    /// Add a buffer to the body, maintaining 8-byte alignment
    /// Returns the buffer descriptor that was added
    pub fn addBuffer(self: *Self, data: []const u8) ?BufferDesc {
        const aligned_len = alignTo8(data.len);

        if (self.offset + aligned_len > self.buffer.len) {
            return null;
        }

        const desc = BufferDesc{
            .offset = @intCast(self.offset),
            .length = @intCast(data.len),
        };

        // Copy data
        @memcpy(self.buffer[self.offset..][0..data.len], data);

        // Zero-fill padding
        const padding = aligned_len - data.len;
        if (padding > 0) {
            @memset(self.buffer[self.offset + data.len ..][0..padding], 0);
        }

        self.offset += aligned_len;

        // Track the descriptor
        if (self.buffer_desc_count >= MAX_BUFFERS) return null;
        self.buffer_descs[self.buffer_desc_count] = desc;
        self.buffer_desc_count += 1;

        return desc;
    }

    /// Add an empty buffer (for nullable columns with no data)
    pub fn addEmptyBuffer(self: *Self) ?BufferDesc {
        const desc = BufferDesc{
            .offset = @intCast(self.offset),
            .length = 0,
        };

        if (self.buffer_desc_count >= MAX_BUFFERS) return null;
        self.buffer_descs[self.buffer_desc_count] = desc;
        self.buffer_desc_count += 1;

        return desc;
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
};

/// Encode EventColumns to Arrow IPC bytes using dynamic schema.
///
/// This is the main entry point for converting parsed events to Arrow IPC.
/// Uses schema_config for:
/// 1. Schema message bytes (from DynamicSchemaConfig.schema_bytes)
/// 2. Dynamic RecordBatch encoding via encodeRecordBatchDynamic
///
/// NOTE: This implementation maps EventColumns' 4 fields (id, type, timestamp, value)
/// to the schema. Extended schemas with flattened value.* fields require
/// parser changes to extract nested JSON fields into separate columns.
///
/// Parameters:
/// - cols: Parsed event columns
/// - schema_config: Dynamic schema configuration with schema bytes and field metadata
/// - output: Output buffer (must be large enough for IPC stream)
///
/// Returns: Slice of valid IPC bytes, or error if buffer too small
pub fn writeArrowIpcFromColumnsWithSchema(
    cols: *const columns.EventColumns,
    schema_config: *const dynamic_schema.DynamicSchemaConfig,
    output: []u8,
) ![]const u8 {
    const row_count: i64 = @intCast(cols.count);

    // Validate output buffer is adequate
    const output_len = output.len;
    if (output_len < 4096) return error.BufferTooSmall;

    var write_offset: usize = 0;

    // 1. Write schema message from schema_config
    const schema_len = schema_config.writeSchemaMessage(output);
    if (schema_len == 0) return error.BufferTooSmall;
    write_offset += schema_len;

    // 2. Build body using DynamicBodyBuilder
    // Use remaining output buffer space for body building
    const body_buffer_start = output_len / 2;
    if (body_buffer_start <= write_offset) return error.BufferTooSmall;

    var body_builder = dynamic_record_batch.DynamicBodyBuilder.init(output[body_buffer_start..]);

    // Count null values in value column
    var value_null_count: i64 = 0;
    var i: u32 = 0;
    while (i < cols.count) : (i += 1) {
        if (!cols.hasValue(i)) {
            value_null_count += 1;
        }
    }

    // Map EventColumns 4 fields to DynamicColumn entries:
    // col 0: id (Utf8)
    const id_offsets_bytes = std.mem.sliceAsBytes(cols.id_offsets[0 .. cols.count + 1]);
    const id_col = dynamic_record_batch.DynamicColumn.utf8(
        0,
        false,
        null,
        id_offsets_bytes,
        cols.id_data[0..cols.id_data_len],
    );
    if (!body_builder.addColumn(id_col, row_count, 0)) return error.BufferTooSmall;

    // col 1: type (Utf8)
    const type_offsets_bytes = std.mem.sliceAsBytes(cols.type_offsets[0 .. cols.count + 1]);
    const type_col = dynamic_record_batch.DynamicColumn.utf8(
        1,
        false,
        null,
        type_offsets_bytes,
        cols.type_data[0..cols.type_data_len],
    );
    if (!body_builder.addColumn(type_col, row_count, 0)) return error.BufferTooSmall;

    // col 2: timestamp (Int64)
    const ts_bytes = std.mem.sliceAsBytes(cols.timestamps[0..cols.count]);
    const ts_col = dynamic_record_batch.DynamicColumn.int64(
        2,
        false,
        null,
        ts_bytes,
    );
    if (!body_builder.addColumn(ts_col, row_count, 0)) return error.BufferTooSmall;

    // col 3: value (Binary nullable)
    const null_bitmap_bytes = (cols.count + 7) / 8;
    const value_offsets_bytes = std.mem.sliceAsBytes(cols.value_offsets[0 .. cols.count + 1]);
    const value_col = dynamic_record_batch.DynamicColumn.binary(
        3,
        true,
        cols.value_nulls[0..null_bitmap_bytes],
        value_offsets_bytes,
        cols.value_data[0..cols.value_data_len],
    );
    if (!body_builder.addColumn(value_col, row_count, value_null_count)) return error.BufferTooSmall;

    // 3. Encode RecordBatch using dynamic encoder
    const body_len = body_builder.bodyLength();
    const rb_output = output[write_offset..body_buffer_start];
    const rb_written = dynamic_record_batch.encodeRecordBatchDynamic(
        rb_output,
        row_count,
        &body_builder,
        body_len,
    );
    if (rb_written == 0) return error.BufferTooSmall;
    write_offset += rb_written;

    // 4. Write EOS marker
    if (write_offset + EOS_MARKER.len > output_len) return error.BufferTooSmall;
    @memcpy(output[write_offset..][0..EOS_MARKER.len], &EOS_MARKER);
    write_offset += EOS_MARKER.len;

    return output[0..write_offset];
}

/// Write Arrow IPC from DynamicColumns (extraction path)
///
/// Encodes N-column DynamicColumns to Arrow IPC format.
/// Similar to writeArrowIpcFromColumnsWithSchema but reads from DynamicColumns.
pub fn writeArrowIpcFromDynamicColumns(
    dyn_cols: *const columns.DynamicColumns,
    schema_config: *const dynamic_schema.DynamicSchemaConfig,
    output: []u8,
) ![]const u8 {
    const row_count: i64 = @intCast(dyn_cols.count);

    // Validate output buffer is adequate
    const output_len = output.len;
    if (output_len < 4096) return error.BufferTooSmall;

    var write_offset: usize = 0;

    // 1. Write schema message from schema_config
    const schema_len = schema_config.writeSchemaMessage(output);
    if (schema_len == 0) return error.BufferTooSmall;
    write_offset += schema_len;

    // 2. Build body using DynamicBodyBuilder
    const body_buffer_start = output_len / 2;
    if (body_buffer_start <= write_offset) return error.BufferTooSmall;

    var body_builder = dynamic_record_batch.DynamicBodyBuilder.init(output[body_buffer_start..]);

    // 3. Iterate over columns and add each to body
    for (schema_config.field_metadata, 0..) |meta, col_idx| {
        const col_storage = &dyn_cols.columns[col_idx];

        // Count nulls for this column
        var null_count: i64 = 0;
        var i: u32 = 0;
        const col_idx_u32: u32 = @intCast(col_idx);
        while (i < dyn_cols.count) : (i += 1) {
            if (dyn_cols.isNull(col_idx_u32, i)) {
                null_count += 1;
            }
        }

        // Build DynamicColumn based on Arrow type
        const validity_bytes = if (meta.isNullable())
            col_storage.validity[0 .. (dyn_cols.count + 7) / 8]
        else
            null;

        switch (meta.arrow_type) {
            .Utf8 => {
                const offsets_bytes = std.mem.sliceAsBytes(col_storage.offsets.?[0 .. dyn_cols.count + 1]);

                const dc = dynamic_record_batch.DynamicColumn.utf8(
                    @intCast(col_idx),
                    meta.isNullable(),
                    validity_bytes,
                    offsets_bytes,
                    col_storage.data.?[0..col_storage.data_len],
                );
                if (!body_builder.addColumn(dc, row_count, null_count)) return error.BufferTooSmall;
            },
            .Int => {
                const values_bytes = std.mem.sliceAsBytes(col_storage.fixed_i64.?[0..dyn_cols.count]);

                const dc = dynamic_record_batch.DynamicColumn.int64(
                    @intCast(col_idx),
                    meta.isNullable(),
                    validity_bytes,
                    values_bytes,
                );
                if (!body_builder.addColumn(dc, row_count, null_count)) return error.BufferTooSmall;
            },
            .FloatingPoint => {
                const values_bytes = std.mem.sliceAsBytes(col_storage.fixed_f64.?[0..dyn_cols.count]);

                const dc = dynamic_record_batch.DynamicColumn.float64(
                    @intCast(col_idx),
                    meta.isNullable(),
                    validity_bytes,
                    values_bytes,
                );
                if (!body_builder.addColumn(dc, row_count, null_count)) return error.BufferTooSmall;
            },
            .Bool => {
                const values_bytes = col_storage.bool_data.?[0 .. (dyn_cols.count + 7) / 8];

                const dc = dynamic_record_batch.DynamicColumn.boolean(
                    @intCast(col_idx),
                    meta.isNullable(),
                    validity_bytes,
                    values_bytes,
                );
                if (!body_builder.addColumn(dc, row_count, null_count)) return error.BufferTooSmall;
            },
            .Binary => {
                const offsets_bytes = std.mem.sliceAsBytes(col_storage.offsets.?[0 .. dyn_cols.count + 1]);

                const dc = dynamic_record_batch.DynamicColumn.binary(
                    @intCast(col_idx),
                    meta.isNullable(),
                    validity_bytes,
                    offsets_bytes,
                    col_storage.data.?[0..col_storage.data_len],
                );
                if (!body_builder.addColumn(dc, row_count, null_count)) return error.BufferTooSmall;
            },
            .Null => {
                // Null-only column - just validity buffer (all nulls)
                const dc = dynamic_record_batch.DynamicColumn{
                    .field_idx = @intCast(col_idx),
                    .arrow_type = .Null,
                    .nullable = true,
                    .validity = validity_bytes,
                    .data = &.{}, // Empty slice
                    .offsets = null,
                };
                if (!body_builder.addColumn(dc, row_count, @intCast(dyn_cols.count))) return error.BufferTooSmall;
            },
        }
    }

    // 4. Encode RecordBatch using dynamic encoder
    const body_len = body_builder.bodyLength();
    const rb_output = output[write_offset..body_buffer_start];
    const rb_written = dynamic_record_batch.encodeRecordBatchDynamic(
        rb_output,
        row_count,
        &body_builder,
        body_len,
    );
    if (rb_written == 0) return error.BufferTooSmall;
    write_offset += rb_written;

    // 5. Write EOS marker
    if (write_offset + EOS_MARKER.len > output_len) return error.BufferTooSmall;
    @memcpy(output[write_offset..][0..EOS_MARKER.len], &EOS_MARKER);
    write_offset += EOS_MARKER.len;

    return output[0..write_offset];
}

// ============================================================================
// Tests
// ============================================================================

test "IpcWriter writes EOS" {
    var buffer: [512]u8 = undefined;
    var writer = IpcWriter.init(&buffer);

    const ok = writer.writeEOS();
    try std.testing.expect(ok);
    try std.testing.expectEqual(@as(usize, 8), writer.bytesWritten());

    // Check EOS bytes
    const output = writer.getOutput();
    try std.testing.expectEqualSlices(u8, &EOS_MARKER, output[0..8]);
}

test "BodyBuilder alignment" {
    var buffer: [256]u8 = undefined;
    var builder = BodyBuilder.init(&buffer);

    // Add 5-byte buffer (should be padded to 8)
    const data1 = [_]u8{ 1, 2, 3, 4, 5 };
    const desc1 = builder.addBuffer(&data1);
    try std.testing.expect(desc1 != null);
    try std.testing.expectEqual(@as(i64, 0), desc1.?.offset);
    try std.testing.expectEqual(@as(i64, 5), desc1.?.length);

    // Next buffer should start at offset 8
    const data2 = [_]u8{ 6, 7, 8 };
    const desc2 = builder.addBuffer(&data2);
    try std.testing.expect(desc2 != null);
    try std.testing.expectEqual(@as(i64, 8), desc2.?.offset);
    try std.testing.expectEqual(@as(i64, 3), desc2.?.length);

    // Body length should be 16 (8 + 8)
    try std.testing.expectEqual(@as(usize, 16), builder.bodyLength());
}

test "writeArrowIpcFromColumnsWithSchema basic" {
    var cols = try columns.EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    // Add a simple event
    _ = cols.addEvent("test-id-001", "testEvent", 1705315800000000, "{\"key\":\"value\"}");

    // Create minimal schema config
    // Minimal schema stub for structural testing - writeArrowIpcFromColumnsWithSchema
    // copies this directly, so any valid schema message works. Real schema comes from TypeScript.
    // Use the continuation marker + metadata size + minimal FlatBuffer
    const schema_bytes = [_]u8{
        0xFF, 0xFF, 0xFF, 0xFF, // continuation marker
        0x08, 0x00, 0x00, 0x00, // metadata size = 8 (minimal)
        0x00, 0x00, 0x00, 0x00, // placeholder FlatBuffer data
        0x00, 0x00, 0x00, 0x00,
    };
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Binary, .nullable = 1 }, // value
    };
    var config = try dynamic_schema.DynamicSchemaConfig.init(std.testing.allocator, &schema_bytes, &fields, 4);
    defer config.deinit();

    // Write IPC
    var output: [8192]u8 = undefined;
    const ipc = try writeArrowIpcFromColumnsWithSchema(&cols, &config, &output);

    // Verify starts with continuation marker (from schema message)
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), std.mem.readInt(u32, ipc[0..4], .little));

    // Verify ends with EOS marker (last 8 bytes: 0xFFFFFFFF 0x00000000)
    const eos_start = ipc.len - 8;
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), std.mem.readInt(u32, ipc[eos_start..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 0x00000000), std.mem.readInt(u32, ipc[eos_start + 4 ..][0..4], .little));

    // Verify IPC is non-trivial (schema + recordbatch + eos)
    try std.testing.expect(ipc.len > schema_bytes.len + 100);
}

test "writeArrowIpcFromColumnsWithSchema multiple events" {
    var cols = try columns.EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    // Add multiple events
    _ = cols.addEvent("id-1", "type-a", 1000000, "{\"a\":1}");
    _ = cols.addEvent("id-2", "type-b", 2000000, null);
    _ = cols.addEvent("id-3", "type-c", 3000000, "{\"c\":3}");

    // Create minimal schema config
    const schema_bytes = [_]u8{
        0xFF, 0xFF, 0xFF, 0xFF,
        0x08, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 },
        .{ .arrow_type = .Utf8, .nullable = 0 },
        .{ .arrow_type = .Int, .nullable = 0 },
        .{ .arrow_type = .Binary, .nullable = 1 },
    };
    var config = try dynamic_schema.DynamicSchemaConfig.init(std.testing.allocator, &schema_bytes, &fields, 4);
    defer config.deinit();

    // Write IPC
    var output: [16384]u8 = undefined;
    const ipc = try writeArrowIpcFromColumnsWithSchema(&cols, &config, &output);

    // Should produce valid IPC bytes
    try std.testing.expect(ipc.len > 100);

    // Verify EOS at end
    try std.testing.expectEqualSlices(u8, &EOS_MARKER, ipc[ipc.len - 8 .. ipc.len]);
}
