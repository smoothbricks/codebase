# Changed-key feed, write sequence and watch log

## 1. Contract

The VM records which keys of which tracked sources one RETE pass may have changed. An entry is a may-have-changed claim:
for every tracked source whose overflow bit is clear, the pass's entries hold a superset of the keys whose RETE-bindable
row changed since the source's change flags were last cleared. A source whose overflow bit is set changed as a whole.
Nothing is ever overwritten. The RETE layer owns the tracked mask, the pass boundaries, recording its derived-fact
writes and everything it seeds from the feed.

Two facts outlive a pass. Each slot's write sequence (§6) says whether a slot was written between two of its states. The
watch log (§7) keeps, for the slots a host watches, every write's change with the key's prior value across passes, so a
host can name what changed in a slot between a past write sequence and the current state without copying the slot.

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
- struct-map row writes: the upsert family, each recording only a row its pick writes (an upsert-last-if's predicate
  decides, [upsert-last-if](06-struct-map-upsert-last-if.md) §3), counts fold, guarded upsert (its row and the companion
  claim it consumes), probe upsert, card-one scatter, guarded-scatter stamp, removal and stored-row sweep;
- two-key upsert-last and tuple upserts, exact-pair removal and prefix removal, with the prior value;
- TTL eviction of a hash map (with its value), set or bitmap.

A row a removal cut skips ([log-order removal](05-log-order-removal.md)) writes nothing and records nothing.

## 4. Overflow

A source overflows (counted once per pass in `FeedWork::overflowed`) when:

- the entry buffer is full: the entry is dropped and counted in `FeedWork::dropped`;
- a write names no key: bitmap algebra, a bitmap import (`Vm::bitmap_import`) and nested-container writes;
- a fact index has no chain: indices at or past the bound fact count overflow bit 0;
- its change flag is already set as the pass begins: a write the feed did not see (bridge fact syncs, direct map and set
  calls between passes);
- the slot is a kind no kernel records keys for (aggregate, array, condition tree, scalar, ordered list), or the image
  does not hold it: overflowed at every pass begin.

The fourth rule needs every live writer of a slot to set its change flag. A bitmap import is the known exception: it
sets no flag, so an import made between passes is invisible to the feed; one made while the feed is armed overflows its
slot by the second rule. It does advance the slot's write sequence (§6) and overflows the slot's watch log (§7).

## 5. Lifecycle and memory

`feed_bind(tracked, facts, cap)` runs once per program: chains for every tracked slot and, when bit 0 is tracked, for
fact indices below `facts`, and room for `cap` entries. It is the feed's only allocation and refuses as `OutOfMemory`; a
tracked slot past the `u16` is `InvalidProgram`. `feed_begin_pass(state)` runs before the pass's evictions: it forgets
the last pass's entries, seeds overflow (§4) and arms. `feed_end_pass` disarms at the final flag clear and on every
early return. The capacity dry run records nothing. A rolled-back attempt restores the flags, and the next begin
discards its entries. `feed()` reads the entries, chains and overflow; `feed_record_fact` records a derived-fact write;
`feed_work` reports `recorded`, `dropped` and `overflowed` since the bind.

## 6. Write sequence

Slot metadata bytes 48..56 hold `WRITE_SEQ`, a little-endian `u64` (`SlotMetaOffset::WRITE_SEQ`; `SLOT_META_SIZE` is
56). Every site that sets a slot's change flag advances it by one in the same write, through
`SlotMetaView::mark(state, flag)`, the one writer of both. It counts writes, not values: a slot whose sequence moved
between two states of one history was written between them, and one whose sequence stayed was not. A write that changes
nothing a reader can observe sets no flag and does not advance it: a hash-map upsert of an equal value, a TTL-only
refresh of a held member, an eviction that finds only stale index entries, a seal. A bitmap import advances it without a
flag (§4). Change flags clear at every RETE pass; the sequence never clears.

Init writes 0 and growth copies it. A logical import starts the rebuilt slot at 0, so two sequences compare only along
one history: a state and a later state folded from it.

The sequence is journaled with the flag. The change-flag boundary saves every slot's sequence when it opens and journals
each moved one, one 8-byte record, when it closes. A direct capture writer outside a boundary names
`(meta + WRITE_SEQ, 8)` beside its flag byte, and a boundary that owns the flags owns those bytes too. A rolled-back
write restores the sequence.

## 7. Watch log

The watch log keeps, for each slot a host watches, the change of every write since the slot's coverage began, across
passes, in a ring per slot. It answers a slot's net change between a past write sequence and the current state.

**Watching.** `watch(state, slot, entries, image_bytes)` starts logging a slot with a ring of `entries` entries and
`image_bytes` bytes of prior images, reserved there and refused as `OutOfMemory`, or keeps an existing log. It answers
the slot's coverage: the sequence above which the log is complete, the slot's current sequence when logging starts.
`unwatch(slot)` releases a slot's log. Recording never allocates.

**Entries.** Each write of a watched slot appends, after it lands, one entry per key it wrote:
`key1 u32 · key2 u32 · stamp u64 · kind u8` and the key's prior value, stamped with the slot's sequence after the write.
`kind` is the change-flag bit of what the write did to that key. The prior value is what a reader decodes the key to
before the write:

- a hash map's `u32` value;
- an aggregate's or scalar's 8-byte value, in one keyless entry;
- a struct-map or two-key row's image (below);
- none for a set, bitmap or ordered-list member, whose `kind` says whether it was held.

An insertion has no prior: an unset scalar, whose timestamp is still −∞, decodes to nothing, so its first write inserts
it. A hash map's or u32 scalar's prior is its stored lane; a host whose lane holds interned ids counts the log's priors
(`log_priors(slot)`) as references of those ids while the log holds them. A bitmap patch is one write: it logs the
members it added or removed, which its witness names. The sites are §3's plus the aggregate, scalar and ordered-list
writes, an ordered-list member keyed by its position. They record whether or not the feed is armed: the log does not
depend on the feed's pass, and a capacity dry run records nothing.

**Row images** are self-contained. An image is the row's bytes as stored, then, in descriptor order, each set field that
names text or an arena payload (an interned string, an arena array, a value-object payload), length-prefixed: a `u32`
length, then the bytes. An interned-string array is written element by element. An image never holds an interner id or
an arena offset that a later write, growth, seal or interner release could invalidate, so it decodes for as long as the
log holds it. A row image whose interned text the kernel's strings cannot resolve, or that is larger than the slot's
whole image ring, is not recorded; the write appends a whole-slot entry instead. A host that watches a slot holding
interned text passes the interner its kernels write through.

**Eviction and overflow.** An entry that does not fit the ring evicts the slot's oldest entries until it fits, and
coverage moves to the last evicted entry's stamp. Which entries above a `since` survive therefore depends only on the
writes made after it, so two engines that fold the same writes after `since` answer it alike. A write that names no key
(bitmap algebra and import, nested containers) appends one whole-slot entry with no key and no prior. Images are
self-contained, so growth and seal of a slot leave its log whole.

**Replay and settling.** Stamps within a slot's ring only increase. A write stamped at or below the slot's newest entry
records nothing: the state was rewound and is folding the same writes again, which the log already holds, so a host that
rewinds to replay one history (a replay engine returning to a phase's basis) leaves the log as it is. A host that
replaces the state with a different future (restoring a speculation basis whose attempt it abandons) calls
`log_settle(state)`, which drops every entry stamped above its slot's current sequence and clamps coverage to it. An
undo rollback settles by itself. A host that copies a state into another VM to continue its history there (a replay
fork) copies the log with it: `log_clone_from(other)` replaces this VM's log with `other`'s rings, images and coverage.
A rollforward of a delta segment is a keyless write of every watched slot whose sequence it moves: it appends that
slot's whole-slot entry at the new sequence. A rollback of one settles by itself, as an undo rollback does.

**Checkpoints.** A host that stores a state image and later restores it in another VM stores its watched slots' logs
with it, so a restored engine answers a slot change asked across the restore exactly as the engine that wrote the image
would have. `log_export(state, slot, above, out)` appends one little-endian record of the slot's log as of `state`: its
ring sizes, its coverage raised to `above`, and the entries stamped in `(above, seq]`, `seq` the slot's sequence in
`state`, each with its prior (a 24-byte header `entries u32 · image_bytes u32 · coverage u64 · count u32 · zero u32`,
then per entry a 24-byte header `key1 u32 · key2 u32 · stamp u64 · kind u8 · held u8 · zero u16 · prior_len u32` and its
prior, zero-padded to 8; `WatchLogOffset`, `WatchLogEntryOffset`). The record has no version: the build that wrote it
reads it, beside its image. `log_import(state, slot, bytes)` watches the slot with those sizes and holds those entries
and coverage, replacing any log the slot had, and answers the coverage; over the same image, `net_changes` then answers
as the exporting VM did for every `since` at or above `above`. It refuses a malformed record, and one naming a coverage
or stamp above the slot's sequence in `state` (an image older than the log it is paired with), and a refusal changes
nothing.

**Net changes.** `net_changes(state, slot, since, strings, out)` answers the slot's net change over the window from
`since` to the slot's sequence in `state`; entries stamped above that sequence belong to a later state and are ignored.
It answers `Overflowed` when `since` is below the slot's coverage or a whole-slot entry lies in the window. Otherwise,
for each key whose first entry in the window is stamped above `since`, it takes that entry's prior and whether the key
is present in `state`, and keeps the key only when they differ:

- an insert: no prior, and the key is present;
- a delete: a prior, and the key is absent;
- an update: a prior, the key is present, and the current value's image differs from the prior's.

A key written and written back nets to nothing. An aggregate or scalar answers at most one keyless change, keyed `0, 0`.
`out` (`NetChanges`) holds each change's key lanes, kind and prior image in record order; the host reads the current
value from the state. Each record is little-endian and starts at a multiple of 8 (`NET_CHANGE_ALIGN`): a 16-byte header
(`NET_CHANGE_HEADER_SIZE`, offsets `NetChangeOffset`)
`key1 u32 · key2 u32 · kind u8 · has_prior u8 · pad u16 · prior_len u32`, `kind` 1 insert, 2 update or 3 delete
(`NetChangeKind`), then `prior_len` prior bytes, zero-padded. A slot the VM does not watch answers `Overflowed`.

## 8. Behavioral acceptance

Tests write through each kind of site in §3 and require exactly its key, change and prior value; require that untracked
sources and an unarmed feed record nothing; force a small capacity and require whole-source overflow with counted drops;
require that a write made while unarmed, a set derived-facts flag and an unkeyable tracked kind overflow at the next
begin; that an unchained fact overflows bit 0; that a rolled-back attempt leaves no entries and its retry records the
same ones; that a counts fold and an upsert-last-if whose admission dry-runs them each record once; that a row an
upsert-last-if's predicate refuses records nothing; that removal-cut-skipped rows record nothing; and that a bound feed
records warm passes with zero allocations.

The write sequence and the watch log: a test of each §3 site and of the aggregate, scalar and ordered-list writes
requires that the slot's sequence advances by one per write call and the log records its key, kind and prior value; that
an equal hash-map value, a TTL-only refresh and a seal advance nothing; that a rolled-back attempt restores the sequence
and leaves no log entries; that a row image with an interned string, an array and a value-object payload decodes after
the slot grows and after the string's id is released; that `net_changes` answers insert, update and delete, drops a key
written and written back, and answers `Overflowed` for a `since` below coverage, after a keyless write and after the
ring evicted an entry above `since`, and after a whole-slot entry in the window; that a rewound state refolding the same
writes records nothing new and that `net_changes` ignores entries above the state's sequence; that `log_settle` drops
entries above a restored sequence; and that a watched slot records warm passes with zero allocations.
