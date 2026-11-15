# bitmosaic: a tiny set costs ten bytes of framing and two a member

`packages/bitmosaic/crates/bitmosaic` (README §Native v1, `src/wire.rs` `write_frame`, `forest_header_len`).

## What is measured

A consumer is weighing bitmosaic for the set of columns present in one stored log entry: a few to a few dozen ordinals
below a hundred. Measured with the crate on the real cases:

- 4 members of 83: 18 bytes. A plain bitmap of 83 bits is 11.
- 13 members of 70: 22 to 24 bytes. A plain bitmap is 9.
- 20 members of 83: 23 bytes.
- all 83: 14 bytes (one run).

The 18 bytes of the four-member image, from the format: 4 identification (`BMS` and the control byte), 1 body length, 1
cardinality, 1 chunk count, 2 the key of the one chunk, 1 the container kinds, 8 the members as `u16`. Ten bytes are
framing and a member costs two.

The owner (2026-11-15): "That seems more like a defect in bitmosaic for very small bitmaps, can that be optimized?"

## What to change

1. **An embedded form with no identification.** A set held inside a value whose format already says "a bitmosaic set
   follows" needs no `BMS` and no self-describing control beyond the mode. Saves three to four bytes.
2. **No chunk directory for one chunk at key zero.** Every set whose members are below 65,536 is that case. The chunk
   count and the key go. Saves three bytes.
3. **A small container.** The smallest member encoding today is Array, `u16` each. For a chunk whose members span under
   256: one byte a member (a base and `u8` offsets), or a bitmap window of `ceil(span / 8)` bytes, whichever is smaller,
   chosen by the same price comparison as the other kinds.

By arithmetic, not measured: about 7 bytes for 4 of 83, about 12 for 13 of 70, about 5 for all 83.

## Constraints

- Native v1 rejects unknown control modes and kinds 5 to 7. The new modes and kind are additions with their admission
  rules, readers, `patch` support and oracle coverage; an image written today stays readable.
- Borrowed readers allocate nothing; the small container keeps that.
- TypeScript has no bitmosaic reader of its own (a consumer reaches it through wasm). If the column set is read in
  TypeScript, say what that reader is.

## Done when

The four cases above are measured again in the crate's benches or tests and the README states the new forms.
