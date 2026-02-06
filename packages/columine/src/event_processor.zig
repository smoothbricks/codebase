//! Columine EventProcessor - JSON to Arrow IPC
//!
//! Entry point for the Parse + Compact pipeline.
//! Provides createLogEntry for JSON-to-Arrow-IPC conversion.
//!
//! This is the generic columine version WITHOUT dedup (bloom filter, checkpoint).
//!
//! NOTE: Msgpack support is deferred. JSON is the only supported format.

const std = @import("std");
const builtin = @import("builtin");

// =============================================================================
// Module imports
// =============================================================================

// Parsing modules - JSON to columnar format
pub const parsing = struct {
    pub const columns = @import("parsing/columns.zig");
    pub const json_scanner = @import("parsing/json_scanner.zig");
    pub const json_parser = @import("parsing/json_parser.zig");
    pub const json_extractor = @import("parsing/json_extractor.zig");
};

// Arrow module - IPC encoding for output
pub const arrow = struct {
    pub const ipc_writer = @import("arrow/ipc_writer.zig");
    pub const dynamic_schema = @import("arrow/dynamic_schema.zig");
    pub const dynamic_record_batch = @import("arrow/dynamic_record_batch.zig");
};

// =============================================================================
// Re-export commonly used types
// =============================================================================

// Parsing types
pub const EventColumns = parsing.columns.EventColumns;
pub const DynamicColumns = parsing.columns.DynamicColumns;
pub const ColumnStorage = parsing.columns.ColumnStorage;
pub const ColumnType = parsing.columns.ColumnType;
pub const ParseError = parsing.columns.ParseError;
pub const parseJsonEvents = parsing.json_scanner.parseJsonEvents;

// JSON extraction types
pub const JsonParser = parsing.json_parser.JsonParser;
pub const extractJsonEvent = parsing.json_extractor.extractJsonEvent;
pub const ExtractionConfig = parsing.json_extractor.ExtractionConfig;
pub const ExtractionError = parsing.json_extractor.ExtractionError;

// Arrow types
pub const IpcWriter = arrow.ipc_writer.IpcWriter;
pub const BodyBuilder = arrow.ipc_writer.BodyBuilder;
pub const BufferDesc = arrow.dynamic_record_batch.BufferDesc;
pub const FieldNode = arrow.dynamic_record_batch.FieldNode;

// Dynamic schema types
pub const DynamicSchemaConfig = arrow.dynamic_schema.DynamicSchemaConfig;
pub const SignalSchemaField = arrow.dynamic_schema.SignalSchemaField;
pub const ArrowType = arrow.dynamic_schema.ArrowType;

// =============================================================================
// Constants
// =============================================================================

pub const VERSION: u32 = 1;

/// Input format enum for createLogEntry
pub const InputFormat = enum(u8) {
    JSON = 0,
    MSGPACK = 1, // Deferred - returns INVALID_FORMAT if used
    ARROW_PASSTHROUGH = 2,
};

/// Result codes for WASM exports
pub const ResultCode = enum(u32) {
    OK = 0,
    INVALID_HANDLE = 1,
    PARSE_ERROR = 2,
    ENCODE_ERROR = 3,
    OUT_OF_MEMORY = 4,
    INVALID_FORMAT = 5,
};

/// Result header written to output buffer (32 bytes)
/// Layout:
///   0-3:   code (u32)
///   4-7:   arrow_ipc_offset (u32)
///   8-11:  arrow_ipc_len (u32)
///  12-15:  events_processed (u32)
///  16-19:  duplicates_filtered (u32)
///  20-31:  reserved (12 bytes)
pub const ResultHeader = extern struct {
    code: u32,
    arrow_ipc_offset: u32,
    arrow_ipc_len: u32,
    events_processed: u32,
    duplicates_filtered: u32,
    reserved: [12]u8 = [_]u8{0} ** 12,
};

comptime {
    std.debug.assert(@sizeOf(ResultHeader) == 32);
}

// =============================================================================
// EventProcessor instance (Parse + Compact only, no dedup)
// =============================================================================

const EventProcessor = struct {
    event_columns: EventColumns,
    output_buffer: []u8,
    allocator: std.mem.Allocator,
    /// Schema configuration (required for Arrow encoding)
    schema_config: DynamicSchemaConfig,
    /// Dynamic columns for extraction path (null if using base path)
    dynamic_columns: ?DynamicColumns,
    /// Extraction configuration (null if using base path)
    extraction_config: ?parsing.json_extractor.ExtractionConfig,
    /// Work buffer for msgpack serialization (empty if using base path)
    work_buffer: []u8,

    // Buffer sizes for WASM (limited by 8MB fixed buffer)
    const WASM_EVENT_CAPACITY: u32 = 256; // Max events per batch in WASM
    const WASM_OUTPUT_BUFFER_SIZE: usize = 256 * 1024; // 256KB output buffer

    /// Initialize EventProcessor with schema from TypeScript.
    /// Schema is required for Arrow encoding.
    fn init(
        alloc: std.mem.Allocator,
        capacity: u32,
        schema_bytes: []const u8,
        field_meta_ptr: [*]const SignalSchemaField,
        field_count: u32,
    ) !*EventProcessor {
        const self = try alloc.create(EventProcessor);
        errdefer alloc.destroy(self);

        // Use smaller event capacity for WASM to fit in memory
        const event_cap = if (builtin.cpu.arch == .wasm32)
            @min(capacity, WASM_EVENT_CAPACITY)
        else
            capacity;

        // Use smaller output buffer for WASM
        const output_size = if (builtin.cpu.arch == .wasm32)
            WASM_OUTPUT_BUFFER_SIZE
        else
            4 * 1024 * 1024; // 4MB for native

        // Initialize schema config
        const schema_config = try DynamicSchemaConfig.init(
            alloc,
            schema_bytes,
            field_meta_ptr,
            field_count,
        );
        errdefer {
            var config = schema_config;
            config.deinit();
        }

        self.* = EventProcessor{
            .event_columns = try EventColumns.init(alloc, event_cap),
            .output_buffer = try alloc.alloc(u8, output_size),
            .allocator = alloc,
            .schema_config = schema_config,
            .dynamic_columns = null,
            .extraction_config = null,
            .work_buffer = &.{},
        };

        return self;
    }

    /// Initialize EventProcessor with field names for extraction path.
    fn initWithFieldNames(
        alloc: std.mem.Allocator,
        capacity: u32,
        schema_bytes: []const u8,
        field_meta_ptr: [*]const SignalSchemaField,
        field_count: u32,
        field_names_raw: []const u8,
    ) !*EventProcessor {
        const self = try alloc.create(EventProcessor);
        errdefer alloc.destroy(self);

        // Use smaller event capacity for WASM to fit in memory
        const event_cap = if (builtin.cpu.arch == .wasm32)
            @min(capacity, WASM_EVENT_CAPACITY)
        else
            capacity;

        // Use smaller output buffer for WASM
        const output_size = if (builtin.cpu.arch == .wasm32)
            WASM_OUTPUT_BUFFER_SIZE
        else
            4 * 1024 * 1024; // 4MB for native

        // Initialize schema config with field names
        const schema_config = try DynamicSchemaConfig.initWithFieldNames(
            alloc,
            schema_bytes,
            field_meta_ptr,
            field_count,
            field_names_raw,
        );
        errdefer {
            var config = schema_config;
            config.deinit();
        }

        // Initialize extraction structures if schema has extraction fields
        var dyn_cols: ?DynamicColumns = null;
        var extract_config: ?parsing.json_extractor.ExtractionConfig = null;
        var work_buf: []u8 = &.{};

        if (schema_config.has_extraction_fields) {
            dyn_cols = try DynamicColumns.init(alloc, schema_config.field_metadata, event_cap);
            errdefer if (dyn_cols) |*dc| dc.deinit();

            extract_config = try parsing.json_extractor.buildExtractionConfig(
                alloc,
                schema_config.field_metadata,
                schema_config.field_names,
            );
            errdefer if (extract_config) |*ec| parsing.json_extractor.freeExtractionConfig(alloc, ec);

            work_buf = try alloc.alloc(u8, 4096); // Buffer for msgpack serialization
            errdefer alloc.free(work_buf);
        }

        self.* = EventProcessor{
            .event_columns = try EventColumns.init(alloc, event_cap),
            .output_buffer = try alloc.alloc(u8, output_size),
            .allocator = alloc,
            .schema_config = schema_config,
            .dynamic_columns = dyn_cols,
            .extraction_config = extract_config,
            .work_buffer = work_buf,
        };

        return self;
    }

    fn deinit(self: *EventProcessor) void {
        self.event_columns.deinit();
        self.allocator.free(self.output_buffer);
        var config = self.schema_config;
        config.deinit();
        if (self.dynamic_columns) |*dc| dc.deinit();
        if (self.extraction_config) |*ec| parsing.json_extractor.freeExtractionConfig(self.allocator, ec);
        if (self.work_buffer.len > 0) self.allocator.free(self.work_buffer);
        self.allocator.destroy(self);
    }
};

// =============================================================================
// Handle table for WASM (simple array, limited handles)
// =============================================================================

var g_handles: [256]?*EventProcessor = [_]?*EventProcessor{null} ** 256;
var g_next_handle: u32 = 1;

fn allocHandle(ep: *EventProcessor) ?u32 {
    var i: u32 = 0;
    while (i < 256) : (i += 1) {
        const idx = (g_next_handle + i) % 256;
        if (g_handles[idx] == null) {
            g_handles[idx] = ep;
            g_next_handle = (idx + 1) % 256;
            return idx;
        }
    }
    return null;
}

fn getProcessor(handle: u32) ?*EventProcessor {
    if (handle >= 256) return null;
    return g_handles[handle];
}

fn freeHandle(handle: u32) void {
    if (handle < 256) {
        g_handles[handle] = null;
    }
}

// =============================================================================
// Allocator selection (WASM vs native)
// =============================================================================

// WASM allocator selection:
// - wasm_allocator uses memory.grow but has initialization issues
// - page_allocator uses mmap which doesn't exist in WASM
// - We use a simple bump allocator over a static buffer for WASM
const wasm_heap_size = 8 * 1024 * 1024; // 8MB heap
var wasm_heap: [wasm_heap_size]u8 = undefined;
var wasm_fba = std.heap.FixedBufferAllocator.init(&wasm_heap);

const allocator = if (builtin.cpu.arch == .wasm32)
    wasm_fba.allocator()
else
    std.heap.page_allocator;

// =============================================================================
// WASM/FFI Exports
// =============================================================================

/// Get version for compatibility checks
pub export fn ep_version() u32 {
    return VERSION;
}

/// Create EventProcessor with schema (Parse + Compact only, no dedup).
///
/// Schema is required for Arrow encoding. TypeScript defines the schema
/// using Flechette, and Zig stores and uses those bytes for IPC output.
///
/// Parameters:
/// - capacity: expected number of events per batch
/// - schema_ptr: pointer to pre-computed Arrow schema bytes (from Flechette)
/// - schema_len: length of schema bytes
/// - field_meta_ptr: pointer to SignalSchemaField array (4 bytes per field)
/// - field_count: number of fields in schema
///
/// Returns: handle (0 = error)
pub export fn ep_create_with_schema(
    capacity: u32,
    schema_ptr: [*]const u8,
    schema_len: u32,
    field_meta_ptr: [*]const SignalSchemaField,
    field_count: u32,
) u32 {
    const schema_bytes = schema_ptr[0..schema_len];

    const ep = EventProcessor.init(
        allocator,
        capacity,
        schema_bytes,
        field_meta_ptr,
        field_count,
    ) catch return 0;

    return allocHandle(ep) orelse {
        ep.deinit();
        return 0;
    };
}

/// Create EventProcessor with schema AND field names for extraction.
///
/// This version accepts field names which enables the extraction path for
/// schemas with value.* fields. Without field names, Zig cannot match JSON
/// keys to columns.
///
/// Parameters:
/// - capacity: expected number of events per batch
/// - schema_ptr: pointer to pre-computed Arrow schema bytes (from Flechette)
/// - schema_len: length of schema bytes
/// - field_meta_ptr: pointer to SignalSchemaField array (4 bytes per field)
/// - field_count: number of fields in schema
/// - field_names_ptr: pointer to null-terminated field names (e.g., "id\0type\0timestamp\0value.orderId\0")
/// - field_names_len: total length of field_names_ptr including nulls
///
/// Returns: handle (0 = error)
pub export fn ep_create_with_schema_and_names(
    capacity: u32,
    schema_ptr: [*]const u8,
    schema_len: u32,
    field_meta_ptr: [*]const SignalSchemaField,
    field_count: u32,
    field_names_ptr: [*]const u8,
    field_names_len: u32,
) u32 {
    const schema_bytes = schema_ptr[0..schema_len];
    const field_names_raw = field_names_ptr[0..field_names_len];

    const ep = EventProcessor.initWithFieldNames(
        allocator,
        capacity,
        schema_bytes,
        field_meta_ptr,
        field_count,
        field_names_raw,
    ) catch return 0;

    return allocHandle(ep) orelse {
        ep.deinit();
        return 0;
    };
}

/// Destroy EventProcessor instance
pub export fn ep_destroy(handle: u32) void {
    if (getProcessor(handle)) |ep| {
        ep.deinit();
        freeHandle(handle);
    }
}

/// Create log entry from input (Parse + Compact only, no dedup)
///
/// handle: EventProcessor handle
/// input_ptr: pointer to input data
/// input_len: length of input data
/// format: InputFormat (0=JSON, 1=MSGPACK [deferred], 2=ARROW_PASSTHROUGH)
/// output_ptr: pointer to output buffer (caller-provided, min 32 bytes for header)
/// output_len: length of output buffer
///
/// Returns: ResultCode (0=OK)
/// On success, output_ptr contains ResultHeader followed by Arrow IPC bytes.
pub export fn ep_create_log_entry(
    handle: u32,
    input_ptr: [*]const u8,
    input_len: u32,
    format: u8,
    output_ptr: [*]u8,
    output_len: u32,
) u32 {
    const ep = getProcessor(handle) orelse return @intFromEnum(ResultCode.INVALID_HANDLE);

    const input = input_ptr[0..input_len];
    const output = output_ptr[0..output_len];

    if (output_len < @sizeOf(ResultHeader)) {
        return @intFromEnum(ResultCode.OUT_OF_MEMORY);
    }

    const input_format: InputFormat = @enumFromInt(format);
    const arrow_offset: u32 = @sizeOf(ResultHeader);

    // Branch based on extraction path
    if (ep.schema_config.has_extraction_fields) {
        // EXTRACTION PATH: Use DynamicColumns for schemas with value.* fields
        var dyn_cols = &ep.dynamic_columns.?;
        dyn_cols.reset();

        // Parse with extraction
        const extracted: u32 = switch (input_format) {
            .JSON => parsing.json_extractor.extractJsonEvents(
                input,
                &ep.extraction_config.?,
                dyn_cols,
                ep.work_buffer,
            ) catch {
                writeResultHeader(output, .PARSE_ERROR, 0, 0, 0, 0);
                return @intFromEnum(ResultCode.PARSE_ERROR);
            },
            .MSGPACK => {
                writeResultHeader(output, .INVALID_FORMAT, 0, 0, 0, 0);
                return @intFromEnum(ResultCode.INVALID_FORMAT);
            },
            .ARROW_PASSTHROUGH => {
                writeResultHeader(output, .INVALID_FORMAT, 0, 0, 0, 0);
                return @intFromEnum(ResultCode.INVALID_FORMAT);
            },
        };

        // No dedup in columine - all events are processed
        const processed_count = extracted;
        const duplicates: u32 = 0;

        // Encode with DynamicColumns encoder
        const arrow_bytes = arrow.ipc_writer.writeArrowIpcFromDynamicColumns(
            dyn_cols,
            &ep.schema_config,
            output[arrow_offset..],
        ) catch {
            writeResultHeader(output, .ENCODE_ERROR, 0, 0, 0, 0);
            return @intFromEnum(ResultCode.ENCODE_ERROR);
        };

        const arrow_len: u32 = @intCast(arrow_bytes.len);
        writeResultHeader(output, .OK, arrow_offset, arrow_len, processed_count, duplicates);
    } else {
        // BASE PATH: Use EventColumns for standard 4-column schema
        ep.event_columns.reset();

        // Parse with json_scanner
        const parse_result: ParseError = switch (input_format) {
            .JSON => parsing.json_scanner.parseJsonEvents(input, &ep.event_columns, allocator),
            .MSGPACK => .INVALID_MSGPACK,
            .ARROW_PASSTHROUGH => .OK,
        };

        if (parse_result != .OK) {
            writeResultHeader(output, .PARSE_ERROR, 0, 0, 0, 0);
            return @intFromEnum(ResultCode.PARSE_ERROR);
        }

        // No dedup in columine - all events are processed
        const processed_count: u32 = ep.event_columns.count;
        const duplicates: u32 = 0;

        // Encode with EventColumns encoder
        const arrow_bytes = arrow.ipc_writer.writeArrowIpcFromColumnsWithSchema(
            &ep.event_columns,
            &ep.schema_config,
            output[arrow_offset..],
        ) catch {
            writeResultHeader(output, .ENCODE_ERROR, 0, 0, 0, 0);
            return @intFromEnum(ResultCode.ENCODE_ERROR);
        };

        const arrow_len: u32 = @intCast(arrow_bytes.len);
        writeResultHeader(output, .OK, arrow_offset, arrow_len, processed_count, duplicates);
    }

    return @intFromEnum(ResultCode.OK);
}

// =============================================================================
// Helpers
// =============================================================================

fn writeResultHeader(
    output: []u8,
    code: ResultCode,
    arrow_offset: u32,
    arrow_len: u32,
    events_processed: u32,
    duplicates_filtered: u32,
) void {
    const header = ResultHeader{
        .code = @intFromEnum(code),
        .arrow_ipc_offset = arrow_offset,
        .arrow_ipc_len = arrow_len,
        .events_processed = events_processed,
        .duplicates_filtered = duplicates_filtered,
    };
    @memcpy(output[0..@sizeOf(ResultHeader)], std.mem.asBytes(&header));
}

// =============================================================================
// Tests
// =============================================================================

test {
    // Include all submodule tests
    _ = parsing.columns;
    _ = parsing.json_scanner;
    _ = parsing.json_parser;
    _ = parsing.json_extractor;
    _ = arrow.ipc_writer;
    _ = arrow.dynamic_schema;
    _ = arrow.dynamic_record_batch;
}

test "ep_create_log_entry with schema" {
    // Create processor with schema
    // Using a minimal valid schema message for testing
    const schema_bytes = [_]u8{
        0xFF, 0xFF, 0xFF, 0xFF, // continuation marker
        0x08, 0x00, 0x00, 0x00, // metadata size = 8
        0x00, 0x00, 0x00, 0x00, // placeholder FlatBuffer data
        0x00, 0x00, 0x00, 0x00,
    };
    const fields = [_]arrow.dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Binary, .nullable = 1 }, // value
    };

    // No policy param - columine has no dedup
    const handle = ep_create_with_schema(100, &schema_bytes, 16, &fields, 4);
    defer ep_destroy(handle);
    try std.testing.expect(handle != 0);

    // Process JSON
    const input =
        \\[{"id":"test","type":"click","timestamp":1705315800000000}]
    ;
    var output: [8192]u8 = undefined;
    const result = ep_create_log_entry(handle, input.ptr, input.len, 0, &output, output.len);

    // Verify success
    try std.testing.expectEqual(@as(u32, 0), result); // ResultCode.OK

    // Verify result header shows arrow data
    const header = @as(*const ResultHeader, @ptrCast(@alignCast(&output)));
    try std.testing.expect(header.arrow_ipc_len > 0);
    try std.testing.expectEqual(@as(u32, 1), header.events_processed);
    // No dedup in columine
    try std.testing.expectEqual(@as(u32, 0), header.duplicates_filtered);
}
