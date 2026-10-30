# Changed-key feed

## 1. Contract

The VM records which keys of which tracked sources one RETE pass may have changed. An entry is a may-have-changed claim:
for every tracked source whose overflow bit is clear, the pass's entries hold a superset of the keys whose RETE-bindable
row changed since the source's change flags were last cleared. A source whose overflow bit is set changed as a whole.
Nothing is ever overwritten. The RETE layer owns the tracked mask, the pass boundaries, recording its derived-fact
writes and everything it seeds from the feed.

## 2. Sources and entries

Sources use the change-plane layout the RETE layer wakes rules by: bit 0 is the derived-facts table, bit `s + 1` is slot
`s`. The tracked mask and the overflow bitset share it.

An entry is 24 bytes, `repr(C)`: `old u64 · key1 u32 · key2 u32 · source u16 · kind u8 · flags u8 · prev u32`. Flags bit
0 (`HAS_OLD`) says `old` holds the prior value lane; bit 1 (`SOURCE_IS_FACT`) says `source` is a derived-fact index, not
a slot ordinal, so each spans the whole `u16`. `kind` is one change-flag bit: inserted, updated, removed or evicted.
`prev` chains the entries of one source, newest first, with `u32::MAX` ending the chain; `last` names each chain's
newest entry. Fields are private: an entry is built only by `ChangeFeed::record(source, key, change)` with
`Change = Inserted | Updated(Option<u64>) | Removed(Option<u64>) | Evicted(Option<u64>)`, so an insertion with a prior
value cannot exist. `key2` is 0 for a one-key source.

The prior value is the lane a rule binds: a hash map's `u32` value and a two-key map's `field0 | field1 << 32` (present
only when both fields are set). A one-key struct map, a set and a bitmap have none. A derived fact's is the image the
RETE layer passes.

## 3. Recording sites

Each records after its write lands, for its own slot and key, and only while the feed is armed and the slot is tracked
and not overflowed:

- hash map upsert (an equal value rewrites nothing a rule binds and records nothing) and removal, with the prior value;
- hash-set insertion of a member not held, and removal;
- bitmap add and remove: every member the call asked for, because the patch reports counts, not members;
- struct-map row writes: the upsert family, counts fold, guarded upsert (its row and the companion claim it consumes),
  probe upsert, card-one scatter, guarded-scatter stamp, removal and stored-row sweep;
- two-key upsert-last and tuple upserts, exact-pair removal and prefix removal, with the prior value;
- TTL eviction of a hash map (with its value), set or bitmap.

A row a removal cut skips ([log-order removal](05-log-order-removal.md)) writes nothing and records nothing.

## 4. Overflow

A source overflows (counted once per pass in `FeedWork::overflowed`) when:

- the entry buffer is full: the entry is dropped and counted in `FeedWork::dropped`;
- a write names no key: bitmap algebra and nested-container writes;
- a fact index has no chain: indices at or past the bound fact count overflow bit 0;
- its change flag is already set as the pass begins: a write the feed did not see (bridge fact syncs, direct map and set
  calls between passes);
- the slot is a kind no kernel records keys for (aggregate, array, condition tree, scalar, ordered list), or the image
  does not hold it: overflowed at every pass begin.

The fourth rule needs every live writer of a slot to set its change flag. `bitmap_ops::bitmap_import` is the known
exception: it replaces a bitmap's members without setting one, so a host import between passes is invisible to the feed.

## 5. Lifecycle and memory

`feed_bind(tracked, facts, cap)` runs once per program: chains for every tracked slot and, when bit 0 is tracked, for
fact indices below `facts`, and room for `cap` entries. It is the feed's only allocation and refuses as `OutOfMemory`; a
tracked slot past the `u16` is `InvalidProgram`. `feed_begin_pass(state)` runs before the pass's evictions: it forgets
the last pass's entries, seeds overflow (§4) and arms. `feed_end_pass` disarms at the final flag clear and on every
early return. The capacity dry run records nothing. A rolled-back attempt restores the flags, and the next begin
discards its entries. `feed()` reads the entries, chains and overflow; `feed_record_fact` records a derived-fact write;
`feed_work` reports `recorded`, `dropped` and `overflowed` since the bind.

## 6. Behavioral acceptance

Tests write through each kind of site in §3 and require exactly its key, change and prior value; require that untracked
sources and an unarmed feed record nothing; force a small capacity and require whole-source overflow with counted drops;
require that a write made while unarmed, a set derived-facts flag and an unkeyable tracked kind overflow at the next
begin; that an unchained fact overflows bit 0; that a rolled-back attempt leaves no entries and its retry records the
same ones; that a counts fold whose admission dry-runs it records once; that removal-cut-skipped rows record nothing;
and that a bound feed records warm passes with zero allocations.
