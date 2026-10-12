# Struct-map scatter routes

## Shared ordinals

The fused probe scatter (`0x2f`), probe-free scatter (`0x3e`) and guarded scatter (`0x3f`) dispatch through an inline
table of at most 32 routes. The producer assigns dense route ordinals against its complete attribute vocabulary.
Consumers retain those ordinals: selecting fewer destinations never compacts or reorders the table. A row whose ordinal
is `SKIP` (`0xffffffff`) or outside the table is ignored.

Index operands use the program's unsigned LEB128 encoding and the widened body's `u32` encoding. Route kinds remain `0`
for a scalar struct-map field and `1` for set membership. Other kinds are invalid when dispatched.

## No-consumer entries

A route whose destination is not selected retains its table position and encodes `dest_slot = SKIP` (`0xffffffff`). This
is the route-table form of the same no-consumer sentinel, not a third write kind. The entry retains kind `0` or `1`; its
remaining field and value operands are decoded to advance the instruction but are not read or validated as destinations
or sources.

After resolving an in-range ordinal, every scatter skips such an entry before binding its destination, reading its value
or output key, comparing or stamping a guard, reserving undo records, or changing flags. The probe form still reads the
source row to resolve its ordinal. The guarded form's requirement that a kind-0 destination equal the guard slot applies
only to consumed routes. The guard slot itself remains a real allocated struct map; `SKIP` is not a guard-slot index. A
skipped entry names no allocated slot and introduces no destination allocation.

## Admission and verification

Capacity admission ignores a no-consumer destination. That entry contributes no value-column use; any consumed entry
reading the same column still contributes its own use. Neither path treats the sentinel as a real slot or column index.
Instruction-length and body-widening walks still consume every operand, so an ignored middle entry does not hide or
shift a later consumed route.

Behavioral tests cover all three opcodes with an ignored middle ordinal followed by a consumed route, fresh and
undo-enabled execution, unchanged state and journal position for a skipped row, and refusal of an unsupported route
kind. The runtime and TypeScript opcode contracts state the same sentinel and encoding.
