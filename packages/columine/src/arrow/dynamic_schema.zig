//! Dynamic Arrow schema storage and field metadata
//!
//! This module stores pre-computed Arrow schema bytes from TypeScript (Flechette-generated)
//! and field metadata for dynamic buffer computation during RecordBatch encoding.
//!
//! Architecture:
//! 1. TypeScript generates schema bytes using Flechette (proven Arrow implementation)
//! 2. Zig stores those bytes and writes them to IPC output
//! 3. Field metadata enables dynamic buffer count computation
//!
//! This avoids complex FlatBuffer generation in Zig while ensuring Arrow compatibility.

const std = @import("std");

/// Arrow type identifiers matching TypeScript ArrowType enum.
/// These values must match ArrowSchemaDescriptor.ts for correct FFI.
pub const ArrowType = enum(u8) {
    Null = 0,
    Int = 1,
    FloatingPoint = 2,
    Binary = 3,
    Utf8 = 4,
    Bool = 5,
};

/// Field metadata passed from TypeScript for buffer computation.
///
/// This struct is 4 bytes to allow efficient FFI from TypeScript.
/// The layout matches generateFieldMetadata() in generate-dynamic-schema.ts:
/// - byte 0: ArrowType enum value
/// - byte 1: nullable (1 = true, 0 = false)
/// - bytes 2-3: padding for alignment
pub const SignalSchemaField = extern struct {
    /// Arrow type for this field
    arrow_type: ArrowType,
    /// Whether nullable (1 = true, 0 = false)
    nullable: u8,
    /// Padding for 4-byte alignment
    _pad: [2]u8 = [_]u8{0} ** 2,

    /// Returns true if this field allows null values
    pub fn isNullable(self: SignalSchemaField) bool {
        return self.nullable != 0;
    }

    /// Returns the number of buffers this field contributes to a RecordBatch.
    /// Flechette always includes validity buffers even for non-nullable columns.
    pub fn bufferCount(self: SignalSchemaField) u32 {
        // Validity buffer (always present per Flechette convention)
        var count: u32 = 1;

        switch (self.arrow_type) {
            .Utf8, .Binary => {
                // Variable-length: offsets buffer + data buffer
                count += 2;
            },
            .Int, .FloatingPoint, .Bool => {
                // Fixed-length: just data buffer
                count += 1;
            },
            .Null => {
                // Null type has no data buffer
            },
        }

        return count;
    }
};

comptime {
    // Verify struct size matches FFI expectations
    std.debug.assert(@sizeOf(SignalSchemaField) == 4);
    std.debug.assert(@alignOf(SignalSchemaField) == 1);
}

/// Configuration for dynamic schema encoding.
///
/// Stores pre-computed schema bytes from TypeScript and field metadata
/// for dynamic RecordBatch encoding.
pub const DynamicSchemaConfig = struct {
    /// Pre-computed schema FlatBuffer bytes from TypeScript (Flechette-generated).
    /// This is the complete schema message including continuation marker and metadata size.
    schema_bytes: []const u8,
    /// Field metadata for buffer computation
    field_metadata: []const SignalSchemaField,
    /// Number of fields in schema
    field_count: u32,
    /// Allocator for owned data
    allocator: std.mem.Allocator,
    /// True if schema has value.* extraction fields (more than 4 columns)
    /// When true, use dynamic extraction path instead of base 4-column path
    has_extraction_fields: bool,
    /// Field names for JSON key matching (owned, slice of slices)
    field_names: []const []const u8,
    /// Storage for field name strings (owned, concatenated null-terminated)
    field_names_storage: []const u8,

    /// Initialize with data from TypeScript.
    /// Copies all data to owned memory.
    pub fn init(
        allocator: std.mem.Allocator,
        schema_bytes: []const u8,
        field_metadata_ptr: [*]const SignalSchemaField,
        field_count: u32,
    ) !DynamicSchemaConfig {
        // Copy schema bytes to owned memory
        const owned_schema = try allocator.alloc(u8, schema_bytes.len);
        errdefer allocator.free(owned_schema);
        @memcpy(owned_schema, schema_bytes);

        // Copy field metadata to owned memory
        const owned_fields = try allocator.alloc(SignalSchemaField, field_count);
        @memcpy(owned_fields, field_metadata_ptr[0..field_count]);

        // Determine if schema has extraction fields (more than 4 columns)
        // Base schema is: id, type, timestamp, value (4 columns)
        // Extraction schemas have value.* columns and may have more or fewer total columns
        const has_extraction = field_count != 4;

        return DynamicSchemaConfig{
            .schema_bytes = owned_schema,
            .field_metadata = owned_fields,
            .field_count = field_count,
            .allocator = allocator,
            .has_extraction_fields = has_extraction,
            .field_names = &.{}, // Empty slice
            .field_names_storage = &.{}, // Empty slice
        };
    }

    /// Initialize with field names for extraction.
    /// Copies all data to owned memory and parses field names.
    pub fn initWithFieldNames(
        allocator: std.mem.Allocator,
        schema_bytes: []const u8,
        field_metadata_ptr: [*]const SignalSchemaField,
        field_count: u32,
        field_names_raw: []const u8,
    ) !DynamicSchemaConfig {
        // Copy schema bytes to owned memory
        const owned_schema = try allocator.alloc(u8, schema_bytes.len);
        errdefer allocator.free(owned_schema);
        @memcpy(owned_schema, schema_bytes);

        // Copy field metadata to owned memory
        const owned_fields = try allocator.alloc(SignalSchemaField, field_count);
        errdefer allocator.free(owned_fields);
        @memcpy(owned_fields, field_metadata_ptr[0..field_count]);

        // Copy field names storage to owned memory
        const owned_names_storage = try allocator.alloc(u8, field_names_raw.len);
        errdefer allocator.free(owned_names_storage);
        @memcpy(owned_names_storage, field_names_raw);

        // Parse field names (null-terminated strings)
        const owned_names = try parseFieldNames(allocator, owned_names_storage);
        errdefer allocator.free(owned_names);

        // Determine if schema has extraction fields
        const has_extraction = field_count != 4;

        return DynamicSchemaConfig{
            .schema_bytes = owned_schema,
            .field_metadata = owned_fields,
            .field_count = field_count,
            .allocator = allocator,
            .has_extraction_fields = has_extraction,
            .field_names = owned_names,
            .field_names_storage = owned_names_storage,
        };
    }

    /// Parse field names from null-terminated concatenated string.
    /// e.g., "id\0type\0timestamp\0value.orderId\0" -> ["id", "type", "timestamp", "value.orderId"]
    fn parseFieldNames(allocator: std.mem.Allocator, raw: []const u8) ![]const []const u8 {
        // First pass: count how many names
        var count: usize = 0;
        var start: usize = 0;
        for (raw, 0..) |byte, i| {
            if (byte == 0) {
                if (i > start) {
                    count += 1;
                }
                start = i + 1;
            }
        }

        // Allocate slice for names
        const names = try allocator.alloc([]const u8, count);
        errdefer allocator.free(names);

        // Second pass: fill names
        var idx: usize = 0;
        start = 0;
        for (raw, 0..) |byte, i| {
            if (byte == 0) {
                if (i > start) {
                    names[idx] = raw[start..i];
                    idx += 1;
                }
                start = i + 1;
            }
        }

        return names;
    }

    /// Release owned memory
    pub fn deinit(self: *DynamicSchemaConfig) void {
        self.allocator.free(self.schema_bytes);
        self.allocator.free(self.field_metadata);
        if (self.field_names.len > 0) {
            self.allocator.free(self.field_names);
        }
        if (self.field_names_storage.len > 0) {
            self.allocator.free(self.field_names_storage);
        }
        self.* = undefined;
    }

    /// Compute total buffer count for this schema.
    /// This determines the size of the buffers vector in RecordBatch FlatBuffer.
    pub fn computeBufferCount(self: *const DynamicSchemaConfig) u32 {
        var count: u32 = 0;
        for (self.field_metadata) |field| {
            count += field.bufferCount();
        }
        return count;
    }

    /// Get schema message size (for IPC output sizing)
    pub fn schemaMessageSize(self: *const DynamicSchemaConfig) usize {
        return self.schema_bytes.len;
    }

    /// Write schema message to output buffer.
    /// Returns bytes written, or 0 if buffer too small.
    pub fn writeSchemaMessage(self: *const DynamicSchemaConfig, output: []u8) usize {
        if (output.len < self.schema_bytes.len) {
            return 0;
        }
        @memcpy(output[0..self.schema_bytes.len], self.schema_bytes);
        return self.schema_bytes.len;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "SignalSchemaField size and alignment" {
    try std.testing.expectEqual(@as(usize, 4), @sizeOf(SignalSchemaField));
    try std.testing.expectEqual(@as(usize, 1), @alignOf(SignalSchemaField));
}

test "SignalSchemaField isNullable" {
    const nullable_field = SignalSchemaField{ .arrow_type = .Utf8, .nullable = 1 };
    const non_nullable_field = SignalSchemaField{ .arrow_type = .Utf8, .nullable = 0 };

    try std.testing.expect(nullable_field.isNullable());
    try std.testing.expect(!non_nullable_field.isNullable());
}

test "SignalSchemaField bufferCount" {
    // Utf8 (variable-length): validity + offsets + data = 3
    const utf8_field = SignalSchemaField{ .arrow_type = .Utf8, .nullable = 0 };
    try std.testing.expectEqual(@as(u32, 3), utf8_field.bufferCount());

    // Binary (variable-length): validity + offsets + data = 3
    const binary_field = SignalSchemaField{ .arrow_type = .Binary, .nullable = 1 };
    try std.testing.expectEqual(@as(u32, 3), binary_field.bufferCount());

    // Int (fixed-length): validity + data = 2
    const int_field = SignalSchemaField{ .arrow_type = .Int, .nullable = 0 };
    try std.testing.expectEqual(@as(u32, 2), int_field.bufferCount());

    // FloatingPoint (fixed-length): validity + data = 2
    const float_field = SignalSchemaField{ .arrow_type = .FloatingPoint, .nullable = 1 };
    try std.testing.expectEqual(@as(u32, 2), float_field.bufferCount());

    // Bool (fixed-length): validity + data = 2
    const bool_field = SignalSchemaField{ .arrow_type = .Bool, .nullable = 0 };
    try std.testing.expectEqual(@as(u32, 2), bool_field.bufferCount());

    // Null: validity only = 1
    const null_field = SignalSchemaField{ .arrow_type = .Null, .nullable = 1 };
    try std.testing.expectEqual(@as(u32, 1), null_field.bufferCount());
}

test "DynamicSchemaConfig init and deinit" {
    const allocator = std.testing.allocator;

    // Simulate schema bytes from TypeScript
    const schema_bytes = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00 };

    // Field metadata for 4-field schema (id, type, timestamp, value)
    const fields = [_]SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Binary, .nullable = 1 }, // value
    };

    var config = try DynamicSchemaConfig.init(allocator, &schema_bytes, &fields, 4);
    defer config.deinit();

    try std.testing.expectEqual(@as(u32, 4), config.field_count);
    try std.testing.expectEqual(@as(usize, 8), config.schema_bytes.len);
}

test "DynamicSchemaConfig computeBufferCount for 4-field schema" {
    const allocator = std.testing.allocator;

    const schema_bytes = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00 };

    // Standard event schema: id (Utf8), type (Utf8), timestamp (Int64), value (Binary nullable)
    const fields = [_]SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id: 3 buffers
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type: 3 buffers
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp: 2 buffers
        .{ .arrow_type = .Binary, .nullable = 1 }, // value: 3 buffers
    };

    var config = try DynamicSchemaConfig.init(allocator, &schema_bytes, &fields, 4);
    defer config.deinit();

    // Total: 3 + 3 + 2 + 3 = 11 buffers (matches Phase 7's hardcoded count)
    const buffer_count = config.computeBufferCount();
    try std.testing.expectEqual(@as(u32, 11), buffer_count);
}

test "DynamicSchemaConfig computeBufferCount for 5-field schema" {
    const allocator = std.testing.allocator;

    const schema_bytes = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00 };

    // Extended schema: id, type, timestamp, value.orderId, value.amount
    const fields = [_]SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id: 3 buffers
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type: 3 buffers
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp: 2 buffers
        .{ .arrow_type = .Utf8, .nullable = 1 }, // value.orderId: 3 buffers
        .{ .arrow_type = .FloatingPoint, .nullable = 1 }, // value.amount: 2 buffers
    };

    var config = try DynamicSchemaConfig.init(allocator, &schema_bytes, &fields, 5);
    defer config.deinit();

    // Total: 3 + 3 + 2 + 3 + 2 = 13 buffers
    const buffer_count = config.computeBufferCount();
    try std.testing.expectEqual(@as(u32, 13), buffer_count);
}

test "DynamicSchemaConfig writeSchemaMessage" {
    const allocator = std.testing.allocator;

    const schema_bytes = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00 };
    const fields = [_]SignalSchemaField{};

    var config = try DynamicSchemaConfig.init(allocator, &schema_bytes, &fields, 0);
    defer config.deinit();

    // Write to adequate buffer
    var output: [16]u8 = undefined;
    const written = config.writeSchemaMessage(&output);
    try std.testing.expectEqual(@as(usize, 8), written);
    try std.testing.expectEqualSlices(u8, &schema_bytes, output[0..8]);

    // Try too-small buffer
    var small_output: [4]u8 = undefined;
    const small_written = config.writeSchemaMessage(&small_output);
    try std.testing.expectEqual(@as(usize, 0), small_written);
}
