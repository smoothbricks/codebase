// =============================================================================
// Generic Flat Hash Table — typed access over a byte region
// =============================================================================
//
// A hash table stored in a contiguous byte region at a known offset within
// state_base. The table supports u32 keys with open addressing (linear probe),
// EMPTY_KEY/TOMBSTONE sentinels, and 70% max load factor.
//
// Parameterized by Entry: the per-slot storage layout beyond the key.
// Examples:
//   HashSet: Entry = void (keys only)
//   HashMap: Entry = struct { value: u32 }
//   TimestampedMap: Entry = struct { value: u32, timestamp: f64 }
//
// Memory layout at offset:
//   [cap: u32] [size: u32] [keys: u32 × cap] [entries: Entry × cap]
//   (entries omitted when Entry = void)

const std = @import("std");
const types = @import("types.zig");

const EMPTY_KEY = types.EMPTY_KEY;
const TOMBSTONE = types.TOMBSTONE;
const hashKey = types.hashKey;
const align8 = types.align8;

const HDR_CAP: u32 = 0;
const HDR_SIZE: u32 = 4;
pub const HDR_BYTES: u32 = 8;

/// Comptime-generic flat hash table over a byte region.
///
/// `Entry` is the per-slot value type. Use `void` for a set (keys only).
/// Entry must be an extern/packed struct or a primitive for stable layout.
pub fn FlatHashTable(comptime Entry: type) type {
    const has_entries = Entry != void;
    const entry_size: u32 = if (has_entries) @sizeOf(Entry) else 0;

    return struct {
        const Self = @This();

        cap: u32,
        size_ptr: *align(1) u32,
        keys: [*]u32,
        entries: if (has_entries) [*]Entry else void,

        /// Total byte size of a table with the given capacity (including header).
        pub fn byteSize(capacity: u32) u32 {
            return HDR_BYTES + capacity * 4 + capacity * entry_size;
        }

        /// Total byte size WITHOUT header (for top-level slots where cap/size live in metadata).
        pub fn dataSizeNoHeader(capacity: u32) u32 {
            return capacity * 4 + capacity * entry_size;
        }

        /// Bind to an existing table at `state_base + offset` (with inline header).
        pub fn bind(state_base: [*]u8, offset: u32) Self {
            const base = state_base + offset;
            const cap = std.mem.readInt(u32, base[HDR_CAP..][0..4], .little);
            return .{
                .cap = cap,
                .size_ptr = @ptrCast(base + HDR_SIZE),
                .keys = @ptrCast(@alignCast(base + HDR_BYTES)),
                .entries = if (has_entries) @ptrCast(@alignCast(base + HDR_BYTES + cap * 4)) else {},
            };
        }

        /// Bind to a headerless table where cap/size live externally (e.g., in slot metadata).
        /// `data_ptr` points to the start of the keys array (no header prefix).
        pub fn bindExternal(data_ptr: [*]u8, cap: u32, size_ptr: *align(1) u32) Self {
            return .{
                .cap = cap,
                .size_ptr = size_ptr,
                .keys = @ptrCast(@alignCast(data_ptr)),
                .entries = if (has_entries) @ptrCast(@alignCast(data_ptr + cap * 4)) else {},
            };
        }

        /// Bind to a top-level VM slot using SlotMeta.
        pub fn bindSlot(state_base: [*]u8, meta: types.SlotMeta) Self {
            return bindExternal(state_base + meta.offset, meta.capacity, meta.size_ptr);
        }

        /// Initialize a headerless table (for top-level VM slots where cap/size are in metadata).
        /// Fills keys with EMPTY_KEY. Returns the initialized table handle.
        pub fn initExternal(data_ptr: [*]u8, cap: u32, size_ptr: *align(1) u32) Self {
            const keys: [*]u32 = @ptrCast(@alignCast(data_ptr));
            for (0..cap) |i| keys[i] = EMPTY_KEY;
            size_ptr.* = 0;
            return .{
                .cap = cap,
                .size_ptr = size_ptr,
                .keys = keys,
                .entries = if (has_entries) @ptrCast(@alignCast(data_ptr + cap * 4)) else {},
            };
        }

        /// Initialize a new table at `state_base + offset`.
        pub fn init(state_base: [*]u8, offset: u32, capacity: u32) Self {
            const base = state_base + offset;
            std.mem.writeInt(u32, base[HDR_CAP..][0..4], capacity, .little);
            std.mem.writeInt(u32, base[HDR_SIZE..][0..4], 0, .little);
            const keys: [*]u32 = @ptrCast(@alignCast(base + HDR_BYTES));
            for (0..capacity) |i| keys[i] = EMPTY_KEY;
            return Self{
                .cap = capacity,
                .size_ptr = @ptrCast(base + HDR_SIZE),
                .keys = keys,
                .entries = if (has_entries) @ptrCast(@alignCast(base + HDR_BYTES + capacity * 4)) else {},
            };
        }

        pub fn size(self: Self) u32 {
            return self.size_ptr.*;
        }

        pub fn maxLoad(self: Self) u32 {
            return (self.cap * 7) / 10;
        }

        /// Look up key. Returns slot index if found, null otherwise.
        pub fn find(self: Self, key: u32) ?u32 {
            if (key == EMPTY_KEY or key == TOMBSTONE) return null;
            var pos = hashKey(key, self.cap);
            var probes: u32 = 0;
            while (probes < self.cap) : (probes += 1) {
                if (self.keys[pos] == key) return pos;
                if (self.keys[pos] == EMPTY_KEY) return null;
                pos = (pos + 1) & (self.cap - 1);
            }
            return null;
        }

        /// Find insertion position for key.
        pub fn findInsert(self: Self, key: u32) ?struct { pos: u32, found: bool } {
            if (key == EMPTY_KEY or key == TOMBSTONE) return null;
            var pos = hashKey(key, self.cap);
            var probes: u32 = 0;
            while (probes < self.cap) : (probes += 1) {
                const k = self.keys[pos];
                if (k == key) return .{ .pos = pos, .found = true };
                if (k == EMPTY_KEY or k == TOMBSTONE) return .{ .pos = pos, .found = false };
                pos = (pos + 1) & (self.cap - 1);
            }
            return null;
        }

        /// Check if the table contains the key.
        pub fn contains(self: Self, key: u32) bool {
            return self.find(key) != null;
        }

        /// Get entry for key. Returns null if not found.
        pub fn get(self: Self, key: u32) ?if (has_entries) *Entry else void {
            const pos = self.find(key) orelse return null;
            if (has_entries) return &self.entries[pos];
            return {};
        }

        /// Insert key (set semantics). Returns true if newly inserted, false if already present.
        /// Returns null if load factor exceeded (needs growth).
        pub fn insertKey(self: Self, key: u32) ?bool {
            const probe = self.findInsert(key) orelse return null;
            if (probe.found) return false;
            if (self.size() >= self.maxLoad()) return null;
            self.keys[probe.pos] = key;
            self.size_ptr.* += 1;
            return true;
        }

        /// Upsert key with entry. Returns true if newly inserted.
        /// Returns null if load factor exceeded.
        pub fn upsert(self: Self, key: u32, entry: Entry) ?bool {
            if (!has_entries) @compileError("upsert requires Entry != void; use insertKey for sets");
            const probe = self.findInsert(key) orelse return null;
            if (probe.found) {
                self.entries[probe.pos] = entry;
                return false;
            }
            if (self.size() >= self.maxLoad()) return null;
            self.keys[probe.pos] = key;
            self.entries[probe.pos] = entry;
            self.size_ptr.* += 1;
            return true;
        }

        /// Rehash all live entries from self into a new table at dst_offset.
        /// Returns the new table.
        pub fn rehashInto(self: Self, state_base: [*]u8, dst_offset: u32, new_cap: u32) Self {
            var dst = Self.init(state_base, dst_offset, new_cap);
            for (0..self.cap) |i| {
                const k = self.keys[i];
                if (k != EMPTY_KEY and k != TOMBSTONE) {
                    // Find slot in new table (guaranteed to fit since we're growing)
                    var pos = hashKey(k, new_cap);
                    while (dst.keys[pos] != EMPTY_KEY) {
                        pos = (pos + 1) & (new_cap - 1);
                    }
                    dst.keys[pos] = k;
                    if (has_entries) dst.entries[pos] = self.entries[i];
                    dst.size_ptr.* += 1;
                }
            }
            return dst;
        }
    };
}

// Concrete types used by nested containers
pub const HashSet = FlatHashTable(void);
pub const HashMap = FlatHashTable(u32); // key → u32 value
pub const PtrMap = FlatHashTable(u32); // key → u32 arena offset (for outer→inner pointers)

/// HashMap with timestamps (for LATEST/MAX/MIN strategies)
pub const TimestampedEntry = extern struct {
    value: u32,
    _pad: u32 = 0, // alignment padding to 8 bytes
    timestamp: f64,
};
pub const TimestampedMap = FlatHashTable(TimestampedEntry);

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "HashSet — insert, contains, dedup" {
    var buf: [512]u8 align(8) = [_]u8{0} ** 512;
    const tbl = HashSet.init(&buf, 0, 16);

    try testing.expectEqual(@as(u32, 0), tbl.size());

    // Insert
    try testing.expectEqual(true, tbl.insertKey(42).?);
    try testing.expectEqual(@as(u32, 1), tbl.size());

    // Contains
    try testing.expect(tbl.contains(42));
    try testing.expect(!tbl.contains(43));

    // Dedup
    try testing.expectEqual(false, tbl.insertKey(42).?);
    try testing.expectEqual(@as(u32, 1), tbl.size());

    // More inserts
    try testing.expectEqual(true, tbl.insertKey(100).?);
    try testing.expectEqual(true, tbl.insertKey(200).?);
    try testing.expectEqual(@as(u32, 3), tbl.size());
}

test "HashMap — upsert, get, overwrite" {
    var buf: [1024]u8 align(8) = [_]u8{0} ** 1024;
    const tbl = HashMap.init(&buf, 0, 16);

    // Insert
    try testing.expectEqual(true, tbl.upsert(10, 100).?);
    try testing.expectEqual(@as(u32, 1), tbl.size());

    // Get
    const v = tbl.get(10).?;
    try testing.expectEqual(@as(u32, 100), v.*);

    // Overwrite
    try testing.expectEqual(false, tbl.upsert(10, 200).?);
    try testing.expectEqual(@as(u32, 200), tbl.get(10).?.*);

    // Not found
    try testing.expect(tbl.get(99) == null);
}

test "HashSet — load factor triggers null" {
    var buf: [512]u8 align(8) = [_]u8{0} ** 512;
    const tbl = HashSet.init(&buf, 0, 16);

    // 70% of 16 = 11.2, so 12th insert should fail
    var i: u32 = 1;
    while (i <= 11) : (i += 1) {
        try testing.expect(tbl.insertKey(i) != null);
    }
    try testing.expectEqual(@as(u32, 11), tbl.size());

    // 12th should return null (needs growth)
    try testing.expect(tbl.insertKey(12) == null);
}

test "HashSet — rehash into larger table" {
    var buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const src = HashSet.init(&buf, 0, 16);

    var i: u32 = 1;
    while (i <= 10) : (i += 1) {
        _ = src.insertKey(i);
    }
    try testing.expectEqual(@as(u32, 10), src.size());

    // Rehash into larger table at offset 512
    const dst = src.rehashInto(&buf, 512, 32);
    try testing.expectEqual(@as(u32, 10), dst.size());

    // All elements present in destination
    i = 1;
    while (i <= 10) : (i += 1) {
        try testing.expect(dst.contains(i));
    }
    // Elements not in source are not in destination
    try testing.expect(!dst.contains(99));
}

test "HashMap — rehash preserves values" {
    var buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const src = HashMap.init(&buf, 0, 16);

    _ = src.upsert(1, 100);
    _ = src.upsert(2, 200);
    _ = src.upsert(3, 300);

    const dst = src.rehashInto(&buf, 1024, 32);
    try testing.expectEqual(@as(u32, 3), dst.size());
    try testing.expectEqual(@as(u32, 100), dst.get(1).?.*);
    try testing.expectEqual(@as(u32, 200), dst.get(2).?.*);
    try testing.expectEqual(@as(u32, 300), dst.get(3).?.*);
}

test "FlatHashTable — bind to existing table" {
    var buf: [512]u8 align(8) = [_]u8{0} ** 512;
    const created = HashSet.init(&buf, 0, 16);
    _ = created.insertKey(42);
    _ = created.insertKey(99);

    // Bind to same region
    const bound = HashSet.bind(&buf, 0);
    try testing.expectEqual(@as(u32, 2), bound.size());
    try testing.expect(bound.contains(42));
    try testing.expect(bound.contains(99));
}

test "FlatHashTable — byteSize calculation" {
    // HashSet: 8 header + 16 * 4 keys = 72
    try testing.expectEqual(@as(u32, 72), HashSet.byteSize(16));

    // HashMap: 8 header + 16 * 4 keys + 16 * 4 values = 136
    try testing.expectEqual(@as(u32, 136), HashMap.byteSize(16));

    // TimestampedMap: 8 header + 16 * 4 keys + 16 * 16 entries = 328
    try testing.expectEqual(@as(u32, 328), TimestampedMap.byteSize(16));
}

test "EMPTY_KEY and TOMBSTONE are rejected" {
    var buf: [512]u8 align(8) = [_]u8{0} ** 512;
    const tbl = HashSet.init(&buf, 0, 16);

    try testing.expect(tbl.insertKey(EMPTY_KEY) == null);
    try testing.expect(tbl.insertKey(TOMBSTONE) == null);
    try testing.expectEqual(@as(u32, 0), tbl.size());
}

// =============================================================================
// Stress / property tests
// =============================================================================

test "stress — randomized insert 1000 keys, verify all found" {
    var buf: [32768]u8 align(8) = [_]u8{0} ** 32768;
    const tbl = HashSet.init(&buf, 0, 2048);

    var rng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const random = rng.random();

    var inserted: [1024]u32 = undefined;
    var inserted_len: u32 = 0;
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const key = random.intRangeAtMost(u32, 1, 100000);
        if (tbl.insertKey(key)) |was_new| {
            if (was_new) {
                inserted[inserted_len] = key;
                inserted_len += 1;
            }
        }
    }

    // Verify all inserted keys found
    for (inserted[0..inserted_len]) |key| {
        try testing.expect(tbl.contains(key));
    }

    // Verify size matches
    try testing.expectEqual(inserted_len, tbl.size());
}

test "stress — fill to load factor then rehash preserves all" {
    var buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const src = HashSet.init(&buf, 0, 32);

    // Insert 22 keys (just under 70% of 32 = 22.4)
    var i: u32 = 1;
    while (i <= 22) : (i += 1) {
        const result = src.insertKey(i);
        try testing.expect(result != null);
        try testing.expectEqual(true, result.?);
    }
    try testing.expectEqual(@as(u32, 22), src.size());

    // Rehash into cap=64 table at offset 1024
    const dst = src.rehashInto(&buf, 1024, 64);
    try testing.expectEqual(@as(u32, 22), dst.size());

    // Verify all 22 keys present in new table
    i = 1;
    while (i <= 22) : (i += 1) {
        try testing.expect(dst.contains(i));
    }
}

test "property — size always equals count of live keys" {
    var buf: [8192]u8 align(8) = [_]u8{0} ** 8192;
    const tbl = HashMap.init(&buf, 0, 128);

    var count: u32 = 0;
    var i: u32 = 1;
    while (i <= 80) : (i += 1) {
        if (tbl.upsert(i, i * 10)) |was_new| {
            if (was_new) count += 1;
        } else break;
    }

    // Invariant: size == manually tracked count
    try testing.expectEqual(count, tbl.size());

    // Invariant: count of non-sentinel slots == size
    var slot_count: u32 = 0;
    for (0..tbl.cap) |si| {
        if (tbl.keys[si] != EMPTY_KEY and tbl.keys[si] != TOMBSTONE) slot_count += 1;
    }
    try testing.expectEqual(tbl.size(), slot_count);
}

test "stress — HashMap upsert overwrites preserve size" {
    var buf: [4096]u8 align(8) = [_]u8{0} ** 4096;
    const tbl = HashMap.init(&buf, 0, 128);

    // Insert 50 keys with initial values
    var i: u32 = 1;
    while (i <= 50) : (i += 1) {
        const result = tbl.upsert(i, i * 100);
        try testing.expect(result != null);
        try testing.expectEqual(true, result.?);
    }
    try testing.expectEqual(@as(u32, 50), tbl.size());

    // Upsert same 50 keys with different values
    i = 1;
    while (i <= 50) : (i += 1) {
        const result = tbl.upsert(i, i * 200);
        try testing.expect(result != null);
        try testing.expectEqual(false, result.?); // not new
    }

    // Size must still be 50 (not 100)
    try testing.expectEqual(@as(u32, 50), tbl.size());

    // Each key must have the NEW value
    i = 1;
    while (i <= 50) : (i += 1) {
        try testing.expectEqual(i * 200, tbl.get(i).?.*);
    }
}
