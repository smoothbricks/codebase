// =============================================================================
// =============================================================================
//
// Design supports arbitrary nesting: Map<K1, Map<K2, Map<K3, ...V>>>
// Each nested map/set/aggregate is addressed by a "path" of keys.
//
// Path addressing:
//   - Root slots are addressed by slot index (u8)
//   - Nested values use key path: slot[key1][key2][key3]...
//   - Path stored as: depth:u8, key_input_0:u8, key_input_1:u8, ...
//
// Example: map[userId][productId][timestamp] = value
//   MAP_NESTED_UPSERT slot:0, depth:3, keys:[userId_col, productId_col, ts_col], val:amount_col

pub const Opcode = enum(u8) {
    // ═══════════════════════════════════════════════════════════════════
    // Control Flow
    // ═══════════════════════════════════════════════════════════════════
    NOP = 0x00,
    HALT = 0x01,

    // ═══════════════════════════════════════════════════════════════════
    // State Slot Creation (executed once at state init)
    // ═══════════════════════════════════════════════════════════════════
    
    // Flat containers
    SLOT_HASHMAP = 0x10, // slot:u8, capacity_hint:u16, key_type:u8, val_type:u8
    SLOT_HASHSET = 0x11, // slot:u8, capacity_hint:u16, elem_type:u8
    SLOT_AGGREGATE = 0x12, // slot:u8, agg_type:u8 (SUM=1, COUNT=2, MIN=3, MAX=4, AVG=5)
    SLOT_SORTED = 0x13, // slot:u8, capacity_hint:u16, elem_type:u8
    
    // Nested containers - value is another container type
    // Map<K, Map<...>>
    SLOT_MAP_OF_MAP = 0x14, // slot:u8, cap:u16, key_type:u8, nested_slot_template:u8
    // Map<K, Set<...>>
    SLOT_MAP_OF_SET = 0x15, // slot:u8, cap:u16, key_type:u8, elem_type:u8
    // Map<K, Aggregate>
    SLOT_MAP_OF_AGG = 0x16, // slot:u8, cap:u16, key_type:u8, agg_type:u8
    // Map<K, SortedArray>
    SLOT_MAP_OF_SORTED = 0x17, // slot:u8, cap:u16, key_type:u8, elem_type:u8

    // ═══════════════════════════════════════════════════════════════════
    // HashMap Operations (flat - single key)
    // ═══════════════════════════════════════════════════════════════════

    // keyBy(field).keepValue(latest('timestamp'))
    MAP_UPSERT_LATEST = 0x20, // slot:u8, key_input:u8, val_input:u8, ts_input:u8

    // keyBy(field).keepValue(earliest('timestamp'))
    MAP_UPSERT_EARLIEST = 0x21, // slot:u8, key_input:u8, val_input:u8, ts_input:u8

    // keyBy(field).keepValue(max('field'))
    MAP_UPSERT_MAX = 0x22, // slot:u8, key_input:u8, val_input:u8, cmp_input:u8

    // keyBy(field).keepValue(min('field'))
    MAP_UPSERT_MIN = 0x23, // slot:u8, key_input:u8, val_input:u8, cmp_input:u8

    // keyBy(field).keepValue(first)
    MAP_UPSERT_FIRST = 0x24, // slot:u8, key_input:u8, val_input:u8

    // keyBy(field).keepValue(last)
    MAP_UPSERT_LAST = 0x25, // slot:u8, key_input:u8, val_input:u8

    // .removeKeys(stream)
    MAP_REMOVE = 0x26, // slot:u8, key_input:u8

    // .mergeValues((cur, inc) => cur.union(inc)) - for Set-valued maps
    MAP_MERGE_UNION = 0x27, // slot:u8, key_input:u8, val_input:u8

    // ═══════════════════════════════════════════════════════════════════
    // Nested Map Operations - map[k1][k2]...[kN] = v
    // ═══════════════════════════════════════════════════════════════════
    
    // Generic nested upsert with strategy
    // map[k1][k2]...[kN].upsert(value, strategy)
    NESTED_UPSERT_LATEST = 0x28, // slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8, ts:u8
    NESTED_UPSERT_FIRST = 0x29, // slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8
    NESTED_UPSERT_LAST = 0x2A, // slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8
    
    // Nested set insert: map[k1][k2]...[kN].add(elem)
    NESTED_SET_INSERT = 0x2B, // slot:u8, depth:u8, key_inputs[depth]:u8[], elem:u8
    
    // Nested set remove: map[k1][k2]...[kN].delete(elem)
    NESTED_SET_REMOVE = 0x2C, // slot:u8, depth:u8, key_inputs[depth]:u8[], elem:u8
    
    // Nested aggregate update: map[k1][k2]...[kN].update(value)
    NESTED_AGG_UPDATE = 0x2D, // slot:u8, depth:u8, key_inputs[depth]:u8[], val:u8
    
    // Nested map remove: delete map[k1][k2]...[kN]
    NESTED_REMOVE = 0x2E, // slot:u8, depth:u8, key_inputs[depth]:u8[]

    // ═══════════════════════════════════════════════════════════════════
    // HashSet Operations
    // ═══════════════════════════════════════════════════════════════════

    // .unique()
    SET_INSERT = 0x30, // slot:u8, input:u8

    // .remove()
    SET_REMOVE = 0x31, // slot:u8, input:u8

    // .union(other) - modifies slot in-place with elements from other_slot
    SET_UNION = 0x32, // slot:u8, other_slot:u8

    // .intersect(other)
    SET_INTERSECT = 0x33, // slot:u8, other_slot:u8

    // .except(other)
    SET_EXCEPT = 0x34, // slot:u8, other_slot:u8

    // ═══════════════════════════════════════════════════════════════════
    // Aggregate Operations
    // ═══════════════════════════════════════════════════════════════════

    AGG_UPDATE = 0x40, // slot:u8, input:u8
    AGG_UPDATE_FILTERED = 0x41, // slot:u8, input:u8, predicate_expr:u16

    // ═══════════════════════════════════════════════════════════════════
    // Time Filtering (applied to input before other ops)
    // ═══════════════════════════════════════════════════════════════════

    // .within('4 minutes ago')
    FILTER_WITHIN = 0x50, // mask_reg:u8, ts_input:u8, threshold_ms:i64

    // .before(timestamp)
    FILTER_BEFORE = 0x51, // mask_reg:u8, ts_input:u8, threshold_ms:i64

    // .after(timestamp)
    FILTER_AFTER = 0x52, // mask_reg:u8, ts_input:u8, threshold_ms:i64

    // .between(start, end)
    FILTER_BETWEEN = 0x53, // mask_reg:u8, ts_input:u8, start_ms:i64, end_ms:i64

    // ═══════════════════════════════════════════════════════════════════
    // Expressions (for predicates, transforms)
    // ═══════════════════════════════════════════════════════════════════

    EXPR_CONST_I64 = 0x60, // result_reg:u8, value:i64
    EXPR_CONST_F64 = 0x61, // result_reg:u8, value:f64
    EXPR_CMP_GT = 0x62, // result_reg:u8, left:u8, right:u8
    EXPR_CMP_LT = 0x63, // result_reg:u8, left:u8, right:u8
    EXPR_CMP_EQ = 0x64, // result_reg:u8, left:u8, right:u8
    EXPR_CMP_GTE = 0x65, // result_reg:u8, left:u8, right:u8
    EXPR_CMP_LTE = 0x66, // result_reg:u8, left:u8, right:u8
    EXPR_AND = 0x67, // result_reg:u8, left:u8, right:u8
    EXPR_OR = 0x68, // result_reg:u8, left:u8, right:u8
    EXPR_NOT = 0x69, // result_reg:u8, input:u8

    // ═══════════════════════════════════════════════════════════════════
    // JS Callbacks (for e.apply())
    // ═══════════════════════════════════════════════════════════════════

    // Pause VM, call JS function, resume with result
    CALLBACK = 0x70, // result_reg:u8, callback_id:u8, arg_count:u8, arg_regs...:u8[]

    // Batched callback - call JS once with arrays, get array result
    CALLBACK_BATCH = 0x71, // result_input:u8, callback_id:u8, arg_count:u8, arg_inputs...:u8[]

    _,
};

// Aggregate types for SLOT_AGGREGATE
pub const AggType = enum(u8) {
    SUM = 1,
    COUNT = 2,
    MIN = 3,
    MAX = 4,
    AVG = 5,
};

// Column/value types
pub const ValueType = enum(u8) {
    UINT8 = 1,
    UINT16 = 2,
    UINT32 = 3,
    UINT64 = 4,
    INT8 = 5,
    INT16 = 6,
    INT32 = 7,
    INT64 = 8,
    FLOAT32 = 9,
    FLOAT64 = 10,
    BOOL = 11,
    STRING = 12, // interned string index (u32)
};

// Program header magic number "CLM1"
pub const MAGIC: u32 = 0x314D4C43; // "CLM1" in little-endian

pub const ProgramHeader = packed struct {
    magic: u32, // Must be MAGIC
    version: u16, // Protocol version
    num_slots: u8, // Number of state slots
    num_inputs: u8, // Number of input columns
    num_callbacks: u8, // Number of JS callbacks
    flags: u8, // Feature flags
    init_code_len: u16, // Length of init bytecode (slot creation)
    reduce_code_len: u16, // Length of reduce bytecode (per-event ops)
    // Followed by:
    //   - init bytecode (init_code_len bytes)
    //   - reduce bytecode (reduce_code_len bytes)
};
