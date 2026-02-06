// =============================================================================
// =============================================================================
//
// All strings are interned to u32 indices for:
// - Fast HashMap key comparisons (u32 vs string)
// - Arrow dictionary encoding compatibility
// - Cross-batch string deduplication
//
// Layout:
//   data: []u8       - concatenated UTF-8 bytes
//   offsets: []u32   - string boundaries (offset[i] to offset[i+1])
//   count: u32       - number of unique strings
//
// String i has bytes: data[offsets[i]..offsets[i+1]]

const std = @import("std");

pub const StringIntern = struct {
    // String data storage
    data: []u8,
    data_len: u32,
    data_cap: u32,

    // Offsets into data (offsets[count] = data_len for boundary)
    offsets: []u32,
    count: u32,
    offsets_cap: u32,

    // Hash table for deduplication: hash -> first occurrence index
    // Uses open addressing with linear probing
    hash_keys: []u32, // hash values
    hash_indices: []u32, // string indices
    hash_cap: u32,

    const EMPTY: u32 = 0xFFFFFFFF;

    pub fn init(alloc_fn: *const fn (usize) [*]u8, initial_cap: u32) StringIntern {
        const data_cap = initial_cap * 32; // Assume ~32 bytes per string
        const offsets_cap = initial_cap + 1;
        const hash_cap = nextPowerOf2(initial_cap * 2); // 50% load factor

        const data_ptr = alloc_fn(data_cap);
        const offsets_ptr: [*]u32 = @ptrCast(@alignCast(alloc_fn(offsets_cap * 4)));
        const hash_keys_ptr: [*]u32 = @ptrCast(@alignCast(alloc_fn(hash_cap * 4)));
        const hash_indices_ptr: [*]u32 = @ptrCast(@alignCast(alloc_fn(hash_cap * 4)));

        // Initialize hash table to empty
        for (0..hash_cap) |i| {
            hash_keys_ptr[i] = EMPTY;
        }

        // First offset is 0
        offsets_ptr[0] = 0;

        return .{
            .data = data_ptr[0..data_cap],
            .data_len = 0,
            .data_cap = data_cap,
            .offsets = offsets_ptr[0..offsets_cap],
            .count = 0,
            .offsets_cap = offsets_cap,
            .hash_keys = hash_keys_ptr[0..hash_cap],
            .hash_indices = hash_indices_ptr[0..hash_cap],
            .hash_cap = hash_cap,
        };
    }

    fn nextPowerOf2(n: u32) u32 {
        var v = n;
        v -= 1;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v += 1;
        return if (v < 16) 16 else v;
    }

    fn hashBytes(bytes: []const u8) u32 {
        // FNV-1a hash
        var h: u32 = 2166136261;
        for (bytes) |b| {
            h ^= b;
            h *%= 16777619;
        }
        return h;
    }

    /// Intern a string, returning its index.
    /// If the string already exists, returns existing index.
    /// If new, adds to table and returns new index.
    pub fn intern(self: *StringIntern, str_ptr: [*]const u8, str_len: u32) u32 {
        const str = str_ptr[0..str_len];
        const h = hashBytes(str);

        // Look up in hash table
        var slot = h & (self.hash_cap - 1);
        while (true) {
            const key = self.hash_keys[slot];
            if (key == EMPTY) {
                // Not found - insert new string
                return self.insertNew(str, h, slot);
            }
            if (key == h) {
                // Potential match - verify string content
                const idx = self.hash_indices[slot];
                const start = self.offsets[idx];
                const end = self.offsets[idx + 1];
                const existing = self.data[start..end];
                if (std.mem.eql(u8, existing, str)) {
                    return idx; // Found existing
                }
            }
            slot = (slot + 1) & (self.hash_cap - 1);
        }
    }

    fn insertNew(self: *StringIntern, str: []const u8, h: u32, slot: u32) u32 {
        const idx = self.count;
        const str_len: u32 = @intCast(str.len);

        // Copy string data
        const start = self.data_len;
        @memcpy(self.data[start .. start + str_len], str);
        self.data_len += str_len;

        // Update offsets
        self.count += 1;
        self.offsets[self.count] = self.data_len;

        // Update hash table
        self.hash_keys[slot] = h;
        self.hash_indices[slot] = idx;

        return idx;
    }

    /// Get string bytes for an index
    pub fn get(self: *const StringIntern, idx: u32) []const u8 {
        const start = self.offsets[idx];
        const end = self.offsets[idx + 1];
        return self.data[start..end];
    }

    /// Get raw data pointer for Arrow export
    pub fn getDataPtr(self: *const StringIntern) [*]const u8 {
        return self.data.ptr;
    }

    /// Get raw offsets pointer for Arrow export
    pub fn getOffsetsPtr(self: *const StringIntern) [*]const u32 {
        return self.offsets.ptr;
    }

    /// Get total data length
    pub fn getDataLen(self: *const StringIntern) u32 {
        return self.data_len;
    }

    /// Get string count
    pub fn getCount(self: *const StringIntern) u32 {
        return self.count;
    }
};

// =============================================================================
// Exported Functions for direct use
// =============================================================================

var global_intern: ?StringIntern = null;
var intern_heap: [4 * 1024 * 1024]u8 = undefined; // 4MB for string interning
var intern_heap_offset: usize = 0;

fn allocBytes(size: usize) [*]u8 {
    const aligned = (intern_heap_offset + 7) & ~@as(usize, 7);
    intern_heap_offset = aligned + size;
    return intern_heap[aligned..].ptr;
}

export fn intern_create(capacity: u32) void {
    intern_heap_offset = 0;
    global_intern = StringIntern.init(&allocBytes, capacity);
}

export fn intern_string(str_ptr: [*]const u8, str_len: u32) u32 {
    if (global_intern) |*intern| {
        return intern.intern(str_ptr, str_len);
    }
    return 0xFFFFFFFF;
}

export fn intern_get_data_ptr() usize {
    if (global_intern) |*intern| {
        return @intFromPtr(intern.getDataPtr());
    }
    return 0;
}

export fn intern_get_offsets_ptr() usize {
    if (global_intern) |*intern| {
        return @intFromPtr(intern.getOffsetsPtr());
    }
    return 0;
}

export fn intern_get_data_len() u32 {
    if (global_intern) |*intern| {
        return intern.getDataLen();
    }
    return 0;
}

export fn intern_get_count() u32 {
    if (global_intern) |*intern| {
        return intern.getCount();
    }
    return 0;
}
